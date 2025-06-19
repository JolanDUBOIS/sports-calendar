from pathlib import Path
from datetime import datetime

import pytest

from src.data_pipeline.utils import read_yml_file
from src.data_pipeline.data_processing.utils import (
    order_models
)


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
        {"name": "model2", "dependencies": ["production.model3"]},
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

def test_read_yml_file():
    root_path = Path(__file__).parent
    
    data = read_yml_file(root_path / "test_read_yml_files/test_file.yml")
    
    assert data["params"] == {
        "columns_mapping": {
            "direct_paths": {
                "path1": "alpha.beta",
                "path2": "alpha.beta.0.gamma"
            },
            "iterate": {
                "path": "alpha.gamma",
                "columns": {
                    "path3": "delta.epsilon",
                    "path4": "delta.zeta"
                }
            }
        },
        "other_param": "other_value"
    }
    datetime.strptime(data["date_value"], "%Y-%m-%d %H:%M:%S") # Shouldn't raise an error
    
    with pytest.raises(ValueError):
        data = read_yml_file(root_path / "test_read_yml_files/error_test_file.yml")
