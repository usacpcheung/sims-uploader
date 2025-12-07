from fastapi.testclient import TestClient

from app import api


def test_new_upload_form_includes_overlap_acknowledged_field():
    client = TestClient(api.app)
    response = client.get("/ui/uploads/new")

    assert response.status_code == 200
    assert 'name="overlap_acknowledged"' in response.text
    assert 'data-resolution="append"' in response.text
