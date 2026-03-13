"""
Nacionalidade (nationality) CNV schema for NACION3D.CNV: id, descricao, cod (fixed-width layout).
Layout from sample: "     24  Arquipelago midway                                 286"
"""

from cnv_schemas.cnv_schema import CNVField, CNVSchema


class CNVNacionalSchema(CNVSchema):
    """
    CNV schema for nacionalidade (nationality) data from NACION3D.CNV: id, descricao, cod.
    """

    def __init__(self) -> None:
        super().__init__(
            [
                CNVField(pos_start=0, size=7, title="id", type="int"),
                CNVField(pos_start=7, size=43, title="descricao", type="string"),
                CNVField(pos_start=61, size=3, title="cod", type="int"),
            ]
        )
