def infa_repo_sources_sql(database):
    return f"""
        SELECT src.*,SUBJ_NAME
      FROM [{database}].[dbo].[OPB_SRC] src,
      [{database}].[dbo].OPB_SUBJECT sub where (src.subj_id=src.subj_id) 
    """

def infa_repo_targets_sql(database):
    return f"""
        SELECT src.*,SUBJ_NAME
      FROM [{database}].[dbo].[OPB_TRG] src,
      [{database}].[dbo].OPB_SUBJECT sub where (src.subj_id=src.subj_id) 
    """