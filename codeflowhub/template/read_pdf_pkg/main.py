import json
import tempfile

from codeflowhub import get_storage

def render_page_to_image(doc, page_number, out_path, dpi=300, crop_bbox=None):
    import fitz
    from PIL import Image
    page = doc[page_number]
    mat = fitz.Matrix(dpi/72, dpi/72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)

    if crop_bbox:
        scale = dpi / 72
        x0 = int(crop_bbox[0] * scale)
        y0 = int(crop_bbox[1] * scale)
        x1 = int(crop_bbox[2] * scale)
        y1 = int(crop_bbox[3] * scale)
        img = img.crop((x0, y0, x1, y1))
    img.save(out_path, "PNG", icc_profile=None)

def safe_pixmap_to_png_with_fallback(doc, page_number, info, out_path, dpi=200):
    import fitz
    from PIL import Image
    xref = info[0]
    name = info[7]
    try:
        pix = fitz.Pixmap(doc, xref)
        if pix.colorspace is None and pix.n > 1:
            pix = fitz.Pixmap(fitz.csGRAY, pix)
        if pix.alpha:
            pix = fitz.Pixmap(fitz.csRGB, pix)
        if pix.n not in (1, 3):
            pix = fitz.Pixmap(fitz.csRGB, pix)

        mode = "L" if pix.n == 1 else "RGB"
        img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
        img.save(out_path, "PNG", icc_profile=None)
        return True
    except Exception as e:
        print(f"[INFO] Pixmap save failed xref={xref}: {e}")
    page = doc[page_number]
    try:
        bbox = page.get_image_bbox(name)
    except Exception as e:
        print(f"[FAIL] get_image_bbox failed for {name}: {e}")
        return False
    render_page_to_image(doc, page_number, out_path, dpi, crop_bbox=(bbox.x0, bbox.y0, bbox.x1, bbox.y1))
    return True

def extract_text(ocr, doc, page_index, info=None):
    ocr_result = None
    with tempfile.NamedTemporaryFile(suffix='.png') as tf:
        if info is not None:
            safe_pixmap_to_png_with_fallback(doc, page_index, info, tf.name)
        else:
            render_page_to_image(doc, page_index, tf.name)
        result = ocr.predict(tf.name)
        for res in result:
            ocr_result = res.json
    return ocr_result['res']['rec_texts']

def merge_content_by_position(page):
    content_items = []
    text_blocks = page.get_text("blocks")
    for block in text_blocks:
        x0, y0, x1, y1, text, block_no, block_type = block
        content_items.append({
            'type': 'text',
            'content': text.strip(),
            'bbox': (x0, y0, x1, y1),
            'y_pos': y0  # 정렬용 y 좌표
        })

    image_list = page.get_images(full=True)
    for img_info in image_list:
        name = img_info[7]  # 이미지 이름 ('Im0', 'Im1', ...)
        try:
            bbox = page.get_image_bbox(name)
            content_items.append({
                'type': 'image',
                'content': img_info,  # 전체 info 튜플 저장
                'name': name,
                'bbox': (bbox.x0, bbox.y0, bbox.x1, bbox.y1),
                'y_pos': bbox.y0
            })
        except Exception as e:
            print(f"[WARN] Could not get bbox for image {name}: {e}")
            content_items.append({
                'type': 'image',
                'content': img_info,
                'name': name,
                'bbox': None,
                'y_pos': float('inf')  # 맨 뒤로 정렬
            })
    content_items.sort(key=lambda x: x['y_pos'])
    return content_items

def read_pdf(args):
    '''
        PaddleOCR 모델을 이용하여 pdf의 text 내용을 인식합니다.
        모델 캐시 폴더 : /root/.paddlex 
        args['read_pdf_input'] = 'pdf s3 url' # Required
        args['read_pdf_output'] = '인식결과 json s3 url' # Return
    '''
    from paddleocr import PaddleOCR
    import fitz
    if not 'read_pdf_input' in args:
        print('no input so do nothing.')
        return args
    storage = get_storage(args['env'])
    input_pdf = storage.download(args['read_pdf_input'], '/tmp')
    output_json = '/tmp/read_pdf.json'

    ocr = PaddleOCR(
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        lang='korean')

    doc = fitz.open(input_pdf)
    results = []
    for page_index in range(len(doc)):
        print(f"# 페이지 {page_index + 1}/{len(doc)} ")
        page = doc[page_index]
        merged_content = merge_content_by_position(page)
        page_info = {
            'page': page_index+1,
            'content': []
        }
        for item in merged_content:
            if item['type'] == 'text':
                page_info['content'].append({
                    'type': 'text',
                    'text': item['content'],
                    'bbox': item['bbox']
                })
            elif item['type'] == 'image':
                ocr_texts = extract_text(ocr, doc, page_index, item['content'])
                page_info['content'].append({
                    'type': 'image',
                    'name': item['name'],
                    'bbox': item['bbox'],
                    'ocr_texts': ocr_texts
                })
        if len(page_info['content']) == 0:
            ocr_texts = extract_text(ocr, doc, page_index)
            page_info['content'].append({
                'type': 'full_page_image',
                'ocr_texts': ocr_texts
            })
        results.append(page_info)

    with open(output_json, 'w') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    s3_url = storage.upload(output_json)
    return {
        **args,
        'read_pdf_output': s3_url
    }