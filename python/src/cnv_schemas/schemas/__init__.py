"""Predefined CNV schemas."""

from .municipio_schema import CNVMunicipioSchema
from .nacional_schema import CNVNacionalSchema
from .uf_schema import CNVUFSchema

__all__ = ["CNVMunicipioSchema", "CNVNacionalSchema", "CNVUFSchema"]
