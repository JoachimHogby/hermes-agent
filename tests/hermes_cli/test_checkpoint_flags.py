"""Tests for per-run filesystem checkpoint CLI overrides."""


def test_chat_parser_accepts_no_checkpoints():
    from hermes_cli._parser import build_top_level_parser

    parser, _subparsers, _chat_parser = build_top_level_parser()
    args = parser.parse_args(["chat", "--no-checkpoints"])

    assert args.no_checkpoints is True


def test_no_checkpoints_overrides_enabled_config(monkeypatch):
    import cli as cli_mod

    monkeypatch.setitem(cli_mod.CLI_CONFIG, "checkpoints", {"enabled": True})

    instance = cli_mod.HermesCLI(compact=True, no_checkpoints=True)

    assert instance.checkpoints_enabled is False
