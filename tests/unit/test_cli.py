from click.testing import CliRunner

from event_generator import main


def test_no_args_returns_error():
    """CLI must fail when required args are missing."""
    runner = CliRunner()
    result = runner.invoke(main, [])
    assert result.exit_code != 0
    assert "Aborted!" in result.output


