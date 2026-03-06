"""
UF (Unidade Federativa) CNV schema for br_uf.cnv: id, sigla, nome (fixed-width layout).
"""

from cnv_schemas.cnv_schema import CNVField, CNVSchema


class CNVUFSchema(CNVSchema):
    """
    CNV schema for UF (state) data from br_uf.cnv: id, sigla, nome.
    """

    def __init__(self) -> None:
        super().__init__(
            [
                CNVField(pos_start=0, size=7, title="id", type="int"),
                CNVField(pos_start=9, size=2, title="cod_ibge", type="string"),
                CNVField(pos_start=12, size=43, title="nome", type="string"),
            ]
        )
