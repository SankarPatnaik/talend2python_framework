import pytest

from talend2python.runtime.utils import handle_component_error, Talend2PyError


def test_handle_component_error_wraps_exception():
    with pytest.raises(Talend2PyError) as excinfo:
        try:
            raise ValueError("bad")
        except Exception as e:
            handle_component_error("Comp", e)
    assert "Comp" in str(excinfo.value)

