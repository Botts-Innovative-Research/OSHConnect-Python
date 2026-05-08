#  =============================================================================
#  Copyright (c) 2026 Botts Innovative Research Inc.
#  Author: Ian Patterson
#  =============================================================================

"""SensorML 2.0 JSON-encoding structured-field models.

Three types are modeled here:

- `Term` — backs `SystemResource.identifiers` and `.classifiers`. Carries
  ``{definition, label?, value, codeSpace?, name?}`` per the SensorML
  IdentifierTerm / ClassifierTerm shape.
- `Characteristics` and `Capabilities` — back the same-named fields on
  `SystemResource`. Each carries ``{definition?, label?, name?,
  description?, id?, <bucket>: [SWE Common component]}`` where
  ``<bucket>`` is ``characteristics`` for the former and ``capabilities``
  for the latter. Inner components are typed against ``AnyComponent``
  (the SWE Common discriminated union) and validated to carry a
  ``name`` per the SoftNamedProperty binding rule.

Models are permissive on optional metadata (label, name, description,
id, codeSpace) because OSH and other servers vary in what they include
on the wire. They are strict on the fields the spec marks required:
``Term.definition`` / ``Term.value``, and the inner ``AnyComponent``
discriminator/name. ``model_rebuild(force=True)`` runs at the bottom so
the recursive forward-ref machinery (each ``AnyComponent`` arm carries
``list["AnyComponent"]``) doesn't leave a `MockValSer` on the
serializer side — same `model_dump_json` regression the schema models
needed."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .swe_components import AnyComponent, check_named


class Term(BaseModel):
    """SensorML `IdentifierTerm` / `ClassifierTerm` (SensorML 2.0 §7.2.5).

    Used by ``SystemResource.identifiers`` and ``SystemResource.classifiers``.
    The wire shape OSH emits:

    .. code-block:: json

        {"definition": "http://.../SerialNumber",
         "label": "Serial Number",
         "value": "0123456879"}
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    definition: str = Field(..., description="URI naming the term's semantics.")
    value: str = Field(..., description="The identifier/classifier value as a string.")
    label: str = Field(None, description="Optional display label.")
    name: str = Field(None, description="Optional NameToken — the field name in the containing object.")
    code_space: str = Field(None, alias='codeSpace',
                            description="Optional URI naming the codelist `value` belongs to.")


class Characteristics(BaseModel):
    """SensorML `CharacteristicList` (SensorML 2.0 §7.2.7).

    Used by ``SystemResource.characteristics``. The wire shape carries a
    list of inner SWE Common components under the ``characteristics``
    key, where each inner component is bound via SoftNamedProperty and
    must therefore carry a ``name``::

        {"definition": "http://.../OperatingRange",
         "label": "Operating Characteristics",
         "characteristics": [
            {"type": "QuantityRange", "name": "voltage", …},
            {"type": "QuantityRange", "name": "temperature", …}
         ]}
    """
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    definition: str = Field(None,
                            description="URI naming the semantics of the list.")
    label: str = Field(None)
    description: str = Field(None)
    id: str = Field(None)
    name: str = Field(None)
    # Inner SWE Common components — typed against `AnyComponent` so the
    # discriminator on `type` routes to the right concrete subclass.
    characteristics: list[AnyComponent] = Field(...,
                                                description="Inner SWE Common components.")

    @model_validator(mode="after")
    def _characteristics_require_name(self):
        for i, c in enumerate(self.characteristics):
            check_named(c, f"Characteristics.characteristics[{i}]")
        return self


class Capabilities(BaseModel):
    """SensorML `CapabilityList` (SensorML 2.0 §7.2.8).

    Used by ``SystemResource.capabilities``. Isomorphic to
    `Characteristics` but with the inner-array bucket named
    ``capabilities`` instead of ``characteristics``."""
    model_config = ConfigDict(populate_by_name=True, extra='allow')

    definition: str = Field(None,
                            description="URI naming the semantics of the list.")
    label: str = Field(None)
    description: str = Field(None)
    id: str = Field(None)
    name: str = Field(None)
    capabilities: list[AnyComponent] = Field(...,
                                             description="Inner SWE Common components.")

    @model_validator(mode="after")
    def _capabilities_require_name(self):
        for i, c in enumerate(self.capabilities):
            check_named(c, f"Capabilities.capabilities[{i}]")
        return self


# Defense-in-depth: same `MockValSer` rationale as the swe_components.py
# and schema_datamodels.py rebuilds — the recursive forward-ref pattern
# (`list[AnyComponent]` inside Characteristics/Capabilities) needs an
# explicit force-rebuild to fully realize the serializer.
Term.model_rebuild(force=True)
Characteristics.model_rebuild(force=True)
Capabilities.model_rebuild(force=True)


__all__ = ["Term", "Characteristics", "Capabilities"]
