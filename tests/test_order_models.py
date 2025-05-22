import pytest

from src.data_pipeline.data_processing.order_models import ModelOrder


def test_check_circular_dependencies():
    """ Test the _check_circular_dependencies method. """
    non_circular_dependencies = {
        "model1": {"model2"},
        "model2": {"model3"},
        "model3": {"model4"},
        "model4": set(),
    }
    ModelOrder._check_circular_dependencies(non_circular_dependencies)

    circular_dependencies = {
        "model1": {"model2"},
        "model2": {"model3"},
        "model3": {"model1"},
    }
    with pytest.raises(ValueError, match="Circular dependency detected"):
        ModelOrder._check_circular_dependencies(circular_dependencies)

    non_existent_dependencies = {
        "model1": {"model2"},
        "model2": {"model3"},
        "model3": {"non_existent_model"},
    }
    with pytest.raises(ValueError, match="has a dependency on a non-existent model"):
        ModelOrder._check_circular_dependencies(non_existent_dependencies)

def test_get_dependencies():
    models = [
        {"name": "model1", "dependencies": []},
        {"name": "model2", "dependencies": ["intermediate.model1"]},
        {"name": "model3", "dependencies": ["landing.model1"]},
        {"name": "model4", "dependencies": ["intermediate.model2", "intermediate.model3"]},
    ]
    stage = "intermediate"
    expected_dependencies = {
        "model1": set(),
        "model2": {"model1"},
        "model3": set(),
        "model4": {"model2", "model3"},
    }
    dependencies = ModelOrder._get_dependencies(models, stage)
    assert dependencies == expected_dependencies

    error_models = [
        {"name": "model1", "dependencies": ["intermediate.model2"]},
    ]
    stage = "landing"
    with pytest.raises(ValueError, match="has a dependency on a model in a later stage"):
        ModelOrder._get_dependencies(error_models, stage)
    
def test_iteration():
    models = [
        {"name": "model1", "dependencies": [], "data": "data1"},
        {"name": "model2", "dependencies": ["intermediate.model1"], "data": "data2"},
        {"name": "model3", "dependencies": ["landing.model1"], "data": "data3"},
        {"name": "model4", "dependencies": ["intermediate.model5", "intermediate.model3"], "data": "data4"},
        {"name": "model5", "dependencies": ["intermediate.model2", "intermediate.model3"], "data": "data5"},
    ]
    stage = "intermediate"
    model_order = [model for model in ModelOrder(models, stage)]
    expected_order = [
        {"name": "model1", "dependencies": [], "data": "data1"},
        {"name": "model2", "dependencies": ["intermediate.model1"], "data": "data2"},
        {"name": "model3", "dependencies": ["landing.model1"], "data": "data3"},
        {"name": "model5", "dependencies": ["intermediate.model2", "intermediate.model3"], "data": "data5"},
        {"name": "model4", "dependencies": ["intermediate.model5", "intermediate.model3"], "data": "data4"},
    ]
    assert model_order == expected_order
