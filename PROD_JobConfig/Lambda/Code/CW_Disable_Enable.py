import boto3

cw_client = boto3.client('events')


def cw_change_status(cw_name, status):
    try:

        if status == 0:

            response = cw_client.disable_rule(Name=cw_name)
            print("{} Disabled".format(cw_name))

        elif status == 1:

            response = cw_client.enable_rule(Name=cw_name)
            print("{} Enabled".format(cw_name))

        else:
            print("select valid status for {}".format(cw_name))

    except Exception as e:
        print("Error {}".format(e))


def lambda_handler(event, lambda_context):
    try:
        cw_list = ["CW_GLA", "ODP_CW_CD_ENTY_STARTS_IB_INTEL_ENTY", "ODP_CW_CD_ENTY_STARTS_NPS_SPOTFIRE_SCHD",
                   "ODP_CW_CI_CDS_STARTS_CI_CDS_SUMMARY_SCHD_DQF_SCHD", "ODP_CW_CI_CDS_STARTS_IB_UPGRADE_ENTY_CDS",
                   "ODP_CW_CI_CDS_SUMMARY_STARTS_OUTLIERS_CDS_SCHD", "ODP_CW_CI_ENTY_STARTS_CI_CDS_SCHD",
                   "ODP_CW_CI_SR_ENTY_STARTS_CI_ENTY", "ODP_CW_CI_SR_ENTY_STARTS_SR_SCHD",
                   "ODP_CW_CI_SUMMARY_STARTS_CI_SPOTFIRE", "ODP_CW_COMPACTION_STARTS_CD_ENTY",
                   "ODP_CW_DAILY_SCHEDULE_START", "ODP_CW_DAILY_SCHEDULE_START_TRACK1",
                   "ODP_CW_DAILY_SCHEDULE_START_TRACK2", "ODP_CW_DAILY_SCHEDULE_TRACK_A_COMPLETE",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_B_COMPLETE", "ODP_CW_DAILY_SCHEDULE_TRACK_C1",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_C2", "ODP_CW_DAILY_SCHEDULE_TRACK_C3", "ODP_CW_DAILY_SCHEDULE_TRACK_C4",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_C5", "ODP_CW_DAILY_SCHEDULE_TRACK_C6", "ODP_CW_DAILY_SCHEDULE_TRACK_C7",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_C_COMPLETE", "ODP_CW_DAILY_SCHEDULE_TRACK_D1",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_D2", "ODP_CW_DAILY_SCHEDULE_TRACK_D_COMPLETE",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_E1", "ODP_CW_DAILY_SCHEDULE_TRACK_E2", "ODP_CW_DAILY_SCHEDULE_TRACK_E3",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_E4", "ODP_CW_DAILY_SCHEDULE_TRACK_E_COMPLETE",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_F1", "ODP_CW_DAILY_SCHEDULE_TRACK_F2", "ODP_CW_DAILY_SCHEDULE_TRACK_F3",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_F_COMPLETE", "ODP_CW_DAILY_SCHEDULE_TRACK_G1",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_G2", "ODP_CW_DAILY_SCHEDULE_TRACK_G_COMPLETE",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_H1", "ODP_CW_DAILY_SCHEDULE_TRACK_H2", "ODP_CW_DAILY_SCHEDULE_TRACK_H3",
                   "ODP_CW_DAILY_SCHEDULE_TRACK_H4", "ODP_CW_DAILY_SCHEDULE_TRACK_H_COMPLETE",
                   "ODP_CW_DQF_STARTS_DQF_SPOTFIRE", "ODP_CW_IB_CDS_STARTS_IB_SPOTFIRE",
                   "ODP_CW_IB_INTEL_CDS_STARTS_CI_CDS_SCHD", "ODP_CW_IB_INTEL_CDS_STARTS_SR_SCHD_DQF_SCHD",
                   "ODP_CW_IB_INTEL_ENTY_STARTS_IB_INTEL_CDS", "ODP_CW_IB_UPGRADE_ENTY_CDS_STARTS_IB_UPGRADE_SPOTFIRE",
                   "ODP_CW_IB_UPGRADE_STARTS_RULE_STUDIO_WKLY_SCHD", "ODP_CW_MAPP_CDS_STARTS_MAPP_UNSOURCED_SNAPSHOT",
                   "ODP_CW_MAPP_COMP_CONTIGEN_STARTS_MAPP_CDS_SNPST_GLPROD_CONTI",
                   "ODP_CW_MAPP_COMP_ONHAND_INTRDY_STARTS_MAPP_ONHAND_MSTR_INTRADAY",
                   "ODP_CW_MAPP_COMP_ORDER_INTRADAY_STARTS_MAPP_ORDER_MSTR_INTRADAY",
                   "ODP_CW_MAPP_ENTY_STARTS_MAPP_CDS", "ODP_CW_MAPP_MASTER_CONTIGEN_SCHEDULE",
                   "ODP_CW_MAPP_MASTER_CONTIGEN_SCHEDULE_STARTS_MAPP_COMP_CONTIGEN",
                   "ODP_CW_MAPP_MASTER_ONHAND_INTRADAY", "ODP_CW_MAPP_MASTER_ORDER_INTRADAY",
                   "ODP_CW_MAPP_MASTER_STARTS_MAPP_COMPACTION_ONHAND_INTRADAY",
                   "ODP_CW_MAPP_MASTER_STARTS_MAPP_COMPACTION_ORDER_INTRADAY",
                   "ODP_CW_MAPP_ONHAND_MSTR_INTRADAY_STARTS_SPOTFIRE_REFRESH",
                   "ODP_CW_MAPP_ORDER_MSTR_INTRADAY_STARTS_SPOTFIRE_REFRESH",
                   "ODP_CW_MAPP_SNAPSHOT_STARTS_SPOTFIRE_REFRESH",
                   "ODP_CW_MAPP_UNSOURCED_SNAPSHOT_STARTS_MAPP_SNAPSHOT",
                   "ODP_CW_NONSS_IBMF_ENTITY_IBL_STARTS_NONSS_IBMF_SPOTFIRE", "ODP_CW_NPS_CDS_STARTS_NPS_SPOTFIRE_SCHD",
                   "ODP_CW_OUTLIERS_CDS_STARTS_OUTLIERS_SPOTFIRE", "ODP_CW_SR_CDS_STARTS_CI_CDS_SUMMARY_SCHD_DQF_SCHD",
                   "ODP_CW_SR_CDS_STARTS_SR_SPOTFIRE", "ODP_CW_SR_ENTY_STARTS_SR_CDS",
                   "ODP_CW_WEEKLY_MAPP_COMPACTION_GLPROD_ORDER"]
        rules_dict = {}
        for i in cw_list:
            rules_dict.update({i: 1})
            print(rules_dict)

        for cw_name, status in rules_dict.items():
            cw_change_status(cw_name, status)

    except Exception as e:
        print("Error in lambda {}".format(e))
