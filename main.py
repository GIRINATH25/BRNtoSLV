def infer_schema(data):
    if isinstance(data, dict):
        return {key: infer_schema(value) for key, value in data.items()}
    elif isinstance(data, list) and data:
        return [infer_schema(data[0])]
    else:
        return type(data).__name__

json_data = {
    "name": "John",
    "age": 30,
    "email": "john@example.com",
    "address": {
        "city": "New York",
        "zip": "10001"
    },
    "is_active": True
}

schema = infer_schema(json_data)
print(schema)
