"""Temporary smoke coverage for the Python test and package wiring."""


def test_control_plane_package_is_importable() -> None:
    """Remove once substantive package tests provide equivalent import coverage."""
    import sentinelaegisforge_control_plane

    assert sentinelaegisforge_control_plane.__name__ == "sentinelaegisforge_control_plane"
