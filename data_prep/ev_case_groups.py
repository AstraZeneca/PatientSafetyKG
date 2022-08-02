import case_groups as cg


class EudraVigilanceEEACaseGroup(cg.CaseGroupDataSource):
    """
    Defines case group for EudraVigilance EEA Cases
    """

    def __init__(
        self,
        id_column="CaseId",
        source_country_column="Primary Source Country for Regulatory Purposes",
    ):
        super().__init__(
            case_group_id="EU-EEA",
            name="EEA",
            abbreviation="EEA",
            description="Eudravigilance case whose Primary Source Country for Regulatory Purposes is in the European Economic Association",
        )
        self.id_column = id_column
        self.source_country_column = source_country_column

    def generate_edges(self, df, **kwargs):
        result = (
            df.loc[
                (df[self.source_country_column] == kwargs["case_source"]),
                [self.id_column],
            ]
            .drop_duplicates()
            .assign(**{self.case_group_id_column: self.case_group_id})
        )
        return result[[self.case_group_id_column, self.case_id_column]]


class EudraVigilanceNonEEACaseGroup(cg.CaseGroupDataSource):
    """
    Defines case group for EudraVigilance Non-EEA Cases
    """

    def __init__(
        self, source_country_column="Primary Source Country for Regulatory Purposes",
    ):
        super().__init__(
            case_group_id="EU-Non-EEA",
            name="Non-EEA",
            abbreviation="Non-EEA",
            description="Eudravigilance case whose Primary Source Country for Regulatory Purposes is in the European Economic Association",
        )
        self.source_country_column = source_country_column

    def generate_edges(self, df, **kwargs):
        result = (
            df.loc[
                (df[self.source_country_column] == kwargs["case_source"]),
                [self.case_id_column],
            ]
            .drop_duplicates()
            .assign(**{self.case_group_id_column: self.case_group_id})
        )
        return result[[self.case_group_id_column, self.case_id_column]]


###
### Build a pool object for assembling all nodes and edges
### based on EV data
###
EV_CASE_GROUP_POOL = cg.CaseGroupDefinitionPool(name="EV Case Definitions Pool")

###
### Add EEA/Non-EEA Case Groups to the pool
###
EV_CASE_GROUP_POOL.register(EudraVigilanceEEACaseGroup(), case_source="EEA")
EV_CASE_GROUP_POOL.register(EudraVigilanceNonEEACaseGroup(), case_source="Non-EEA")
