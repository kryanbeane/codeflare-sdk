RAY_VERSION = "2.50.1"
"""
The below are used to define the default runtime image for the Ray Cluster.
* For python 3.11:ray:2.50.1-py311-cu212for311
* For python 3.12:ray:2.50.1-py312-cu121for312
"""
CUDA_PY311_RUNTIME_IMAGE = "quay.io/modh/ray@sha256:311runtimeimagesha311"
CUDA_PY312_RUNTIME_IMAGE = "quay.io/modh/ray@sha256:312runtimeimagesha312"

# Centralized image selection
SUPPORTED_PYTHON_VERSIONS = {
    "3.11": CUDA_PY311_RUNTIME_IMAGE,
    "3.12": CUDA_PY312_RUNTIME_IMAGE,
}
