GEO_MODEL_DEVICE_ID = "212e15388f880450"

GEO_MODEL_SUPPORTED_DEVICE_IDS = {
    GEO_MODEL_DEVICE_ID,
    "P001",
}


def is_geo_model_supported_device(device_id):
    return str(device_id) in GEO_MODEL_SUPPORTED_DEVICE_IDS


def resolve_geo_model_device_id(device_id):
    if is_geo_model_supported_device(device_id):
        return GEO_MODEL_DEVICE_ID
    return str(device_id)
