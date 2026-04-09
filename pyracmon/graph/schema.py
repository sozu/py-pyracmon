"""
This module provides the way to generate schema of the graph after being serialized.

Schema is a `TypedDict` type estimated by template property and type hinting annotated to serializer components.
It is obtained statically, thus it is available for, for example, documentation such as JsonSchema.
"""
from collections.abc import Iterator
from typing import TypeVar, Any, Optional, get_args, get_type_hints, cast
try:
    from typing import is_typeddict
except:
    from typing_extensions import is_typeddict
from inspect import signature, Signature
from .graph import GraphView
from .template import GraphTemplate
from .serialize import NodeSerializer, chain_serializers
from .typing import Typeable, issubgeneric, replace_optional_typevar, generate_schema, document_type, decompose_document


def _templateType(t):
    class Template:
        template = t
    return Template


class GraphSchema:
    """
    This class exposes a property to get the schema of serialization result of a graph.

    TODO: Dependency to `GraphSpec` should be replaced in another way.
    """
    def __init__(self, spec: Any, template: GraphTemplate, **serializers: NodeSerializer):
        #: Specification of graph operations.
        self.spec = spec
        #: Graph template to serialize.
        self.template = template
        #: `NodeSerializer`s used for the serialization.
        self.serializers = serializers

    def _return_from(self, prop: GraphTemplate.Property) -> type:
        """
        Get a type the node of passed property will be serialized.
        """
        ns = self.serializers[prop.name]

        # Type of the node entity.
        entity_type = prop.kind
        if isinstance(entity_type, GraphTemplate):
            # GraphTemplate type is ignored because serializer added by sub() resolve the type by iteself.
            entity_type = _templateType(entity_type)

        # Return type of the NodeSerializer.
        ns_type = signature(ns.serializer).return_annotation

        # Return type of base serializer obtained from GraphSpec.
        base = chain_serializers(self.spec.find_serializers(entity_type))
        base_type = signature(base).return_annotation if base else Signature.empty
        #base_type = entity_type if base_type == Signature.empty else base_type

        # If the return type contains a single type parameter, previous type is applied to it.
        # Serializer without return annotation is supposed to return input type as it is.
        def next_resolvable(it: Iterator[type]) -> type:
            while True:
                res = next(it, None)
                if res is None:
                    break
                elif res != Signature.empty:
                    return res
            return Signature.empty

        def resolve(it: Iterator[type]) -> type:
            origin = next_resolvable(it)
            if origin == Signature.empty:
                return origin
            elif issubgeneric(origin, Typeable):
                if not Typeable.is_resolved(origin):
                    param = resolve(it)
                    if param == Signature.empty:
                        # Type parameter is not known.
                        return Signature.empty
                    # Replace type parameter.
                    origin = origin[param] # type: ignore
                return Typeable.resolve(origin, resolve(it), self.spec)
            else:
                args = get_args(origin)
                if args:
                    # origin is generics.
                    type_params = list(filter(lambda ia: isinstance(ia[1], TypeVar), enumerate(args)))
                    # python < 3.10
                    param_num = len(type_params)
                    if param_num == 0:
                        return origin
                    elif param_num == 1:
                        # Replace type parameter
                        param = resolve(it)
                        return origin[param] # type: ignore
                    else:
                        return Signature.empty
                    # python >= 3.10
                    #match len(type_params):
                    #    case 0:
                    #        return origin
                    #    case 1:
                    #        # Replace type parameter
                    #        param = resolve(it)
                    #        return origin[param] # type: ignore
                    #    case _:
                    #        return Signature.empty
                else:
                    return origin

        return resolve(iter([ns_type, base_type, entity_type, entity_type]))

    def schema_of(self, prop: GraphTemplate.Property) -> type:
        """
        Generates structured and documented schema for a template property.

        Args:
            prop: A template property.
        Returns:
            Schema with documentation.
        """
        return_type = self._return_from(prop)

        doc = self.serializers[prop.name]._doc or ""

        # TypedDict type is also a subclass of dict.
        if issubclass(return_type, dict):
            annotations = {}

            for c in filter(lambda c: c.name in self.serializers, prop.children):
                ns = self.serializers[c.name]
                cs = self.schema_of(c)

                t, d = decompose_document(cs)

                if ns.be_merged:
                    if not issubclass(t, dict):
                        raise ValueError(f"Property '{c.name}' is not configured to be serialized into dict.")
                    annotations.update(**{ns.namer(k):t for k, t in get_type_hints(t, include_extras=True).items()})
                elif ns.be_singular:
                    rt = signature(ns.aggregator).return_annotation
                    rt = replace_optional_typevar(rt, cs)
                    annotations[ns.namer(c.name)] = rt
                else:
                    annotations[ns.namer(c.name)] = document_type(list[t], d)

            td_type: Optional[type] = cast(type, return_type) if is_typeddict(return_type) else None

            return document_type(generate_schema(annotations, td_type), doc)
        else:
            return document_type(return_type, doc)

    @property
    def schema(self) -> type:
        """
        Generates `TypedDict` which represents the schema of serialized graph.
        """
        annotations: dict[str, Any] = {}

        def put_root_schema(p: GraphTemplate.Property):
            nonlocal annotations

            ns = self.serializers[p.name]
            dt = self.schema_of(p)

            if ns.be_merged:
                t, d = decompose_document(dt)
                annotations.update(**{ns.namer(k):t_ for k, t_ in get_type_hints(t, include_extras=True).items()})
            elif ns.be_singular:
                rt = signature(ns.aggregator).return_annotation
                rt = replace_optional_typevar(rt, dt)
                annotations[ns.namer(p.name)] = rt
            else:
                t, d = decompose_document(dt)
                annotations[ns.namer(p.name)] = document_type(list[t], d)

        roots = filter(lambda p: p.parent is None and p.name in self.serializers, self.template._properties.values())

        for p in roots:
            put_root_schema(p)

        return generate_schema(annotations)

    def serialize(self, graph: GraphView, **node_params: dict[str, Any]) -> dict[str, Any]:
        """
        Serialize graph into a dictionary.

        Args:
            graph: A view of a graph.
            node_params: Parameters passed to `SerializationContext` and used by *serializer* s.
        Returns:
            Serialization result.
        """
        return self.spec.to_dict(graph, node_params, **self.serializers)