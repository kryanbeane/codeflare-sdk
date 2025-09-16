RAY_VERSION = "2.50.0"
"""
The below are used to define the default runtime image for the Ray Cluster.
* For python 3.11:ray:2.50.0-py311-311cu121
* For python 3.12:ray:2.50.0-py312-312cu121
"""
CUDA_PY311_RUNTIME_IMAGE = "quay.io/modh/ray@sha256:cudapython311image1234"
CUDA_PY312_RUNTIME_IMAGE = "quay.io/modh/ray@sha256:cudapython312image1234"

# Centralized image selection
SUPPORTED_PYTHON_VERSIONS = {
    "3.11": CUDA_PY311_RUNTIME_IMAGE,
    "3.12": CUDA_PY312_RUNTIME_IMAGE,
}
