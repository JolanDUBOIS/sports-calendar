import pytest

from src.data_processing.utils import order_models


def test_order_models():
    models = [
        {"name": "model1", "dependencies": ["landing.model2"]},
        {"name": "model2", "dependencies": []},
        {"name": "model3", "dependencies": ["landing.model1"]},
        {"name": "model4", "dependencies": ["landing.model3", "landing.model2"]},
    ]
    expected_ordered_models = [
        {"name": "model2", "dependencies": []},
        {"name": "model1", "dependencies": ["landing.model2"]},
        {"name": "model3", "dependencies": ["landing.model1"]},
        {"name": "model4", "dependencies": ["landing.model3", "landing.model2"]},
    ]

    ordered_models = order_models(models, "landing")
    assert ordered_models == expected_ordered_models

    models = [
        {"name": "model1", "dependencies": ["landing.model2"]},
        {"name": "model2", "dependencies": []},
        {"name": "model3", "dependencies": ["intermediate.model1"]},
        {"name": "model4", "dependencies": ["intermediate.model3", "intermediate.model2"]},
    ]
    expected_ordered_models = [
        {"name": "model1", "dependencies": ["landing.model2"]},
        {"name": "model2", "dependencies": []},
        {"name": "model3", "dependencies": ["intermediate.model1"]},
        {"name": "model4", "dependencies": ["intermediate.model3", "intermediate.model2"]},
    ]

    ordered_models = order_models(models, "intermediate")
    assert ordered_models == expected_ordered_models

    models = [
        {"name": "model1", "dependencies": ["staging.model2"]},
        {"name": "model2", "dependencies": ["serving.model3"]},
        {"name": "model3", "dependencies": []},
    ]

    with pytest.raises(ValueError):
        order_models(models, "intermediate")

    models = [
        {"name": "model1", "dependencies": ["intermediate.model2"]},
        {"name": "model2", "dependencies": ["intermediate.model3"]},
        {"name": "model3", "dependencies": ["intermediate.model4"]},
        {"name": "model4", "dependencies": ["intermediate.model3", "intermediate.model2"]},
    ]

    with pytest.raises(ValueError):
        order_models(models, "intermediate")
