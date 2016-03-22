CREATE OR REPLACE PACKAGE PKG_LTE_CORE AS 

  /**
  * Author: Carrasco Marcelo
  * Date: 11/02/2016
  * Comment: Paquete para la visualizaci?n de datos de LTE
  */

  FUNCTION F_MMMT (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_MMMT_TAB PIPELINED;
  
  FUNCTION F_SMMT (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SMMT_TAB PIPELINED;
  
  FUNCTION F_UMLM_MMMT (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_UMLM_MMMT_TAB PIPELINED;
  
  FUNCTION FLNS_SGSM (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SGSM_TAB PIPELINED;
  
  FUNCTION F_CONSOLIDADO (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED;
  
END PKG_LTE_CORE;
/


CREATE OR REPLACE PACKAGE BODY PKG_LTE_CORE AS

  FUNCTION F_MMMT(P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_MMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT  PERIOD_START_TIME FECHA,
                       FINS_ID,
                       ROUND(sum(INTRATAU_WO_SGW_CHANGE_SUCC +
                           INTRAMME_TAU_SGW_CHG_SUCC +
                           INTRATAU_WO_SGW_CHANGE_FAIL +
                           EPS_PERIODIC_TAU_SUCC +
                           EPS_PERIODIC_TAU_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3102d,
                       ROUND(sum(EPS_ATTACH_SUCC +
                           EPS_ATTACH_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3100a,
                       ROUND(sum(EPS_DETACH) / avg(PERIOD_DURATION * 60),2) flns_3101a,
                       ROUND(sum(INTERMME_TAU_WO_SGW_CHG_SUCC +
                           INTERMME_TAU_IN_FAIL +
                           INTERMME_TAU_OUT_SUCC +
                           INTERMME_TAU_OUT_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3113a,
                       ROUND(sum(EPS_INTER_TAU_OG_SUCC +
                           EPS_INTER_TAU_OG_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3103b,
                       ROUND(sum(EPS_PATH_SWITCH_X2_SUCC +
                           EPS_X2HO_SGW_CHG_SUCC +
                           EPS_PATH_SWITCH_X2_FAIL +
                           EPS_S1HO_SUCC +
                           INTRAMME_S1HO_SGW_CHG_SUCC +
                           EPS_S1HO_FAIL +
                           INTRAMME_S1HO_SGW_CHG_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3104b,
                       ROUND(sum(INTERMME_S1HO_WO_SGW_CHG_SUCC +
                           INTERMME_S1HO_IN_FAIL +
                           INTERMME_S1HO_OUT_SUCC +
                           INTERMME_S1HO_OUT_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3114a,
                       ROUND(sum(EPS_PAGING_SUCC +
                           EPS_PAGING_FAIL) / avg(PERIOD_DURATION * 60),2) flns_3106a,
                       ROUND(sum(EPS_SERVICE_REQUEST_SUCC +
                           EPS_SERVICE_REQUEST_FAIL -
                           EPS_PAGING_SUCC) / avg(PERIOD_DURATION * 60),2) flns_3218a,
                       ROUND(sum(EPS_SERVICE_REQUEST_SUCC) / avg(PERIOD_DURATION * 60),2) flns_3115a,
                       ROUND(sum(ESR_MO_ATTEMPTS +
                           ESR_MT_ATTEMPTS +
                           ESR_MO_EMERGENCY_ATTEMPTS) / avg(PERIOD_DURATION * 60),2) flns_3108b
                  FROM PCOFNS_PS_MMMT_TA_RAW
                 WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                             AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
                GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_MMMT_OBJ(i.FECHA,i.FINS_ID,i.flns_3102d,i.flns_3100a,i.flns_3101a,i.flns_3113a ,i.flns_3103b ,
                              i.flns_3104b ,i.flns_3114a ,i.flns_3106a ,i.flns_3218a ,i.flns_3115a ,i.flns_3108b));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_MMMT',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_MMMT;
  
  FUNCTION F_SMMT (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT PERIOD_START_TIME FECHA,
                     FINS_ID,
                     ROUND(sum(PDN_CONNECTIVITY_FAILED_UE +
                         PDN_CONNECTIVITY_FAILED_ENB +
                         DDBEARER_UEINIT_ACT_SUCC +
                         DDBEARER_UEINIT_ACT_FAIL +
                         DDBEARER_PGWINIT_ACT_SUCC +
                         DDBEARER_PGWINIT_ACT_FAIL) / avg(period_duration * 60),2) flns_3111a,
                     ROUND(sum(PDN_CONNECT_FAILED_COLLISION +
                         PDN_CONNECT_FAILED_UNSPECIFIED +
                         PDN_CONNECTIVITY_FAILED_MME +
                         PDN_CONNECTIVITY_FAILED_SGW +
                         PDN_CONNECTIVITY_SUCCESSFUL +
                         PDN_CONNECTIVITY_FAILED +
                         DDBEARER_UEINIT_DEACT_SUCC +
                         DDBEARER_UEINIT_DEACT_FAIL +
                         DDBEARER_MMEINIT_DEACT_SUCC +
                         DDBEARER_MMEINIT_DEACT_ABNORM +
                         DDBEARER_PGWINIT_DEACT_SUCC) / avg(period_duration * 60),2) flns_3112a,
                     ROUND(sum(GW_INIT_BEARER_MOD_SUCCESS +
                         GW_INIT_BEARER_MOD_FAILURE +
                         HSS_INIT_BEARER_MOD_SUCCESS +
                         HSS_INIT_BEARER_MOD_FAILURE) / avg(PERIOD_DURATION * 60),2) flns_3172a
                FROM PCOFNS_PS_SMMT_TA_RAW
               WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                          AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
              GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_SMMT_OBJ(i.FECHA,i.FINS_ID ,i.flns_3111a ,i.flns_3112a ,i.flns_3172a));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_SMMT',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_SMMT;
  
  FUNCTION F_UMLM_MMMT (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_UMLM_MMMT_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT X.PERIOD_START_TIME FECHA,
                     X.FINS_ID,
                     ROUND(sum(EPS_TO_3G_GN_ISHO_SUCC +
                         EPS_TO_3G_GN_ISHO_FAIL +
                         EPS_TO_3G_GN_ISHO_TRGT_REJE +
                         EPS_TO_3G_GN_ISHO_ENB_CNCL -
                         SRVCC_PS_AND_CS_HANDOVER_SUCC -
                         SRVCC_PS_AND_CS_HO_3G_FAIL) / avg(Y.PERIOD_DURATION * 60),2) flns_3105b,
                     ROUND(sum(SRVCC_CS_HO_2G_SUCC +
                         SRVCC_CS_HO_2G_FAIL +
                         SRVCC_CS_HANDOVER_SUCC +
                         SRVCC_CS_HO_3G_FAIL +
                         SRVCC_PS_AND_CS_HANDOVER_SUCC +
                         SRVCC_PS_AND_CS_HO_3G_FAIL +
                         SRVCC_EME_HO_2G_SUCC +
                         SRVCC_EME_HO_2G_FAIL +
                         SRVCC_EME_HO_3G_SUCC +
                         SRVCC_EME_HO_3G_FAIL) / avg(X.PERIOD_DURATION * 60),2) flns_3287a
                FROM  PCOFNS_PS_UMLM_FLEXINS_RAW X, 
                      PCOFNS_PS_MMMT_TA_RAW Y
                WHERE X.PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                              AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
                AND X.PERIOD_START_TIME = Y.PERIOD_START_TIME
                AND X.FINS_ID = Y.FINS_ID
                GROUP BY X.PERIOD_START_TIME, X.FINS_ID
                ) loop
      pipe row (FLNS_UMLM_MMMT_OBJ(i.FECHA,i.FINS_ID ,i.flns_3105b ,i.flns_3287a));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_UMLM_MMMT',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END F_UMLM_MMMT;
  
  FUNCTION FLNS_SGSM (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_SGSM_TAB PIPELINED AS
  BEGIN
    FOR i IN (SELECT PERIOD_START_TIME FECHA,
                     FINS_ID,
                     ROUND(sum(SGSAP_UPLINK_SUCC +
                         SGSAP_UPLINK_FAIL +
                         SGSAP_DOWNLINK_SUCC +
                         SGSAP_DOWNLINK_FAIL) / avg(PERIOD_DURATION * 60 * 4),2) flns_3109b
              FROM PCOFNS_PS_SGSM_FLEXINS_RAW
              WHERE PERIOD_START_TIME BETWEEN TO_DATE (P_FINI,'DD.MM.YYYY') 
                                          AND TO_DATE (P_FFIN,'DD.MM.YYYY') + 83999/84000
              GROUP BY PERIOD_START_TIME, FINS_ID
                ) loop
      pipe row (FLNS_SGSM_OBJ(i.FECHA ,i.FINS_ID ,i.flns_3109b));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('FLNS_SGSM',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  END FLNS_SGSM;
  --
  /**
  * Para contener los resultados
  */
  FUNCTION F_CONSOLIDADO (P_FINI VARCHAR2,P_FFIN VARCHAR2 DEFAULT NULL) RETURN FLNS_TAB PIPELINED as
    v_ffin VARCHAR2(10 CHAR) := '';
  BEGIN
    IF P_FFIN IS NULL THEN
      V_FFIN  := P_FINI;
    ELSE
      V_FFIN := P_FFIN;
    END IF;
    
    for i in (SELECT  MMMT.FECHA FECHA,
                      MMMT.FINS_ID FINS_ID,
                      (flns_3105b + flns_3102d + flns_3287a + flns_3100a + flns_3101a + flns_3111a + flns_3112a + 
                      flns_3172a + flns_3113a +  flns_3103b + flns_3104b + flns_3114a + flns_3106a + flns_3218a + 
                      flns_3115a + flns_3108b + flns_3109b) TPS
              FROM 
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3102d ,
                          flns_3100a ,
                          flns_3101a ,
                          flns_3113a ,
                          flns_3103b ,
                          flns_3104b ,
                          flns_3114a ,
                          flns_3106a ,
                          flns_3218a ,
                          flns_3115a ,
                          flns_3108b
                  FROM TABLE(F_MMMT(P_FINI,V_FFIN))) MMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3111a ,
                          flns_3112a ,
                          flns_3172a
                  FROM TABLE(F_SMMT(P_FINI,V_FFIN))) SMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3105b ,
                          flns_3287a
                  FROM TABLE(F_UMLM_MMMT(P_FINI,V_FFIN))) UMLM_MMMT,
                  (SELECT FECHA ,
                          FINS_ID ,
                          flns_3109b
                  FROM TABLE(FLNS_SGSM(P_FINI,V_FFIN))) SGSM
              WHERE MMMT.FECHA = SMMT.FECHA
              AND   SMMT.FECHA = UMLM_MMMT.FECHA
              AND   UMLM_MMMT.FECHA = SGSM.FECHA
              AND   MMMT.FINS_ID = SMMT.FINS_ID
              AND   SMMT.FINS_ID = UMLM_MMMT.FINS_ID
              AND   UMLM_MMMT.FINS_ID = SGSM.FINS_ID
              ) loop
      pipe row (FLNS_OBJ(i.FECHA,i.FINS_ID,i.TPS));
    END loop;
    RETURN;
    exception
      WHEN no_data_needed THEN
        raise;
      WHEN others THEN
        pkg_error_log_new.p_log_error('F_CONSOLIDADO',SQLCODE,SQLERRM,'params P_FINI '||P_FINI||' P_FFIN '||P_FFIN);
  end F_CONSOLIDADO;
  
END PKG_LTE_CORE;
/
