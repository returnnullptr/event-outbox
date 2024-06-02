echo "1) Formatting" && \
ruff format . && \
echo "2) Type checking" && \
mypy . && \
echo "3) Linting" && \
ruff check . && \
echo "4) Testing" && \
pytest . && \
echo "Done!"
