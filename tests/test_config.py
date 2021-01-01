import pytest
import logging
from pyracmon.config import *
from pyracmon.util import Configurable
from pyracmon.model_graph import ConfigurableSpec


class TestDerive:
    def test_derive(self):
        cfg = PyracmonConfiguration(name="test", log_level=10)
        drv = cfg.derive()

        assert (drv.name, drv.log_level) == ("test", 10)

        drv.name = "derived"

        assert (cfg.name, cfg.log_level) == ("test", 10)
        assert (drv.name, drv.log_level) == ("derived", 10)

    def test_derive_overwrite(self):
        cfg = PyracmonConfiguration(name="test", log_level=10)
        drv = cfg.derive(name="derived")

        assert (cfg.name, cfg.log_level) == ("test", 10)
        assert (drv.name, drv.log_level) == ("derived", 10)

    def test_configurable(self):
        ident = lambda x:x
        ef = lambda x:True

        spec = ConfigurableSpec.create()

        cfg = PyracmonConfiguration(name="test", graph_spec=spec)
        drv = cfg.derive()

        assert cfg.graph_spec is not drv.graph_spec
        assert object.__getattribute__(drv, "graph_spec") is not None
        assert len(drv.graph_spec.identifiers) == 1

        drv.graph_spec.add_identifier(str, ident)
        drv.graph_spec.add_entity_filter(str, ef)

        assert len(cfg.graph_spec.identifiers) == 1
        assert len(drv.graph_spec.identifiers) == 2
        assert drv.graph_spec.identifiers[0] == (str, ident)
        assert len(cfg.graph_spec.entity_filters) == 1
        assert len(drv.graph_spec.entity_filters) == 2
        assert drv.graph_spec.entity_filters[0] == (str, ef)

    def test_configurable_overwrite(self):
        ident = lambda x:x

        spec1 = ConfigurableSpec.create()
        spec2 = ConfigurableSpec.create()

        cfg = PyracmonConfiguration(name="test", graph_spec=spec1)
        drv = cfg.derive(graph_spec=spec2)

        assert cfg.graph_spec is not drv.graph_spec
        assert cfg.graph_spec is spec1
        assert drv.graph_spec is spec2

        drv.graph_spec.add_identifier(str, ident)

        assert drv.graph_spec is spec2
        assert len(cfg.graph_spec.identifiers) == 1
        assert len(drv.graph_spec.identifiers) == 2
        assert drv.graph_spec.identifiers[0] == (str, ident)


class TestPyracmon:
    def test_configure(self):
        ident = lambda x:x

        before = default_config()
        save = before

        try:
            with pyracmon() as cfg:
                assert cfg.name == "default"
                assert len(cfg.graph_spec.identifiers) == 1
                assert len(cfg.graph_spec.entity_filters) == 1
                assert len(cfg.graph_spec.serializers) == 1

                cfg.name = "modified"
                cfg.log_level = -1
                cfg.graph_spec.add_identifier(int, ident)

            after = default_config()

            assert before is after
            assert after is not cfg
            assert after.name == "modified"
            assert after.log_level == -1
            assert before.graph_spec is after.graph_spec
            assert len(after.graph_spec.identifiers) == 2
            assert after.graph_spec.identifiers[0] == (int, ident)
        finally:
            before.name = "default"
            before.log_level = logging.DEBUG
            before.graph_spec.identifiers = before.graph_spec.identifiers[1:]