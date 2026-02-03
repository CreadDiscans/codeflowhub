FROM creaddiscans/codeflowhub:base-gpu

RUN pip install lightning==2.5.5 \
    einops huggingface-hub \
    opentelemetry-sdk opentelemetry-exporter-otlp \
    pyannote-core pyannote-database pyannote-metrics pyannote-pipeline \
    torch-audiomentations safetensors asteroid-filterbanks
RUN pip install pyannote-audio --no-deps
