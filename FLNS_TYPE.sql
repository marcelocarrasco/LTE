
create or replace type FLNS_MMMT_OBJ as object (
  FECHA date,
  FINS_ID number,
  flns_3102d number,
  flns_3100a number,
  flns_3101a number,
  flns_3113a number,
  flns_3103b number,
  flns_3104b number,
  flns_3114a number,
  flns_3106a number,
  flns_3218a number,
  flns_3115a number,
  flns_3108b number);
  
create or replace type FLNS_MMMT_TAB is table of FLNS_MMMT_OBJ;

create or replace type FLNS_SMMT_OBJ as object (
FECHA date,
FINS_ID number,
flns_3111a number,
flns_3112a number,
flns_3172a number
);

create or replace type FLNS_SMMT_TAB is table of FLNS_SMMT_OBJ;

create or replace type FLNS_UMLM_MMMT_OBJ as object(
FECHA date,
FINS_ID number,
flns_3105b number,
flns_3287a number
);

create or replace type FLNS_UMLM_MMMT_TAB is table of FLNS_UMLM_MMMT_OBJ;

create or replace type FLNS_SGSM_OBJ as object (
FECHA date,
FINS_ID number,
flns_3109b number
);

create or replace type FLNS_SGSM_TAB is table of FLNS_SGSM_OBJ;

create or replace type FLNS_OBJ as object (
  FECHA date, 
  FINS_ID NUMBER,
  FLNS_VALUE number);
 
create or replace type FLNS_TAB is table of FLNS_OBJ;