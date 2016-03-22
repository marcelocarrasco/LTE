       

SELECT flns_3105b_hour.FECHA FECHA,
       flns_3105b_hour.FINS_ID FINS_ID,
       (flns_3105b_hour.flns_3105b + 
       flns_3102d_hour.flns_3102d + 
       flns_3287a_hour.flns_3287a + 
       flns_3100a_hour.flns_3100a + 
       flns_3101a_hour.flns_3101a + 
       flns_3111a_hour.flns_3111a + 
       flns_3112a_hour.flns_3112a + 
       flns_3172a_hour.flns_3172a + 
       flns_3113a_hour.flns_3113a +
       flns_3103b_hour.flns_3103b + 
       flns_3104b_hour.flns_3104b + 
       flns_3114a_hour.flns_3114a + 
       flns_3106a_hour.flns_3106a + 
       flns_3218a_hour.flns_3218a + 
       flns_3115a_hour.flns_3115a + 
       flns_3108b_hour.flns_3108b + 
       flns_3109b_hour.flns_3109b) TPS
FROM    flns_3105b_hour join  flns_3102d_hour 
              on flns_3105b_hour.fecha = flns_3102d_hour.fecha 
              and flns_3105b_hour.fins_id = flns_3102d_hour.fins_id
        join
        flns_3287a_hour 
              on flns_3287a_hour.fecha = flns_3102d_hour.fecha 
              and flns_3287a_hour.fins_id = flns_3102d_hour.fins_id
        join
        flns_3100a_hour
              on flns_3287a_hour.fecha = flns_3100a_hour.fecha 
              and flns_3287a_hour.fins_id = flns_3100a_hour.fins_id
        join
        flns_3101a_hour
              on flns_3101a_hour.fecha = flns_3100a_hour.fecha 
              and flns_3101a_hour.fins_id = flns_3100a_hour.fins_id
        join
        flns_3111a_hour
              on flns_3101a_hour.fecha = flns_3111a_hour.fecha 
              and flns_3101a_hour.fins_id = flns_3111a_hour.fins_id
        join
        flns_3112a_hour
              on flns_3112a_hour.fecha = flns_3111a_hour.fecha 
              and flns_3112a_hour.fins_id = flns_3111a_hour.fins_id
        join
        flns_3172a_hour
              on flns_3112a_hour.fecha = flns_3172a_hour.fecha 
              and flns_3112a_hour.fins_id = flns_3172a_hour.fins_id
        join
        flns_3113a_hour
              on flns_3113a_hour.fecha = flns_3172a_hour.fecha 
              and flns_3113a_hour.fins_id = flns_3172a_hour.fins_id
        join
        flns_3103b_hour
              on flns_3113a_hour.fecha = flns_3103b_hour.fecha 
              and flns_3113a_hour.fins_id = flns_3103b_hour.fins_id
        join
        flns_3104b_hour
              on flns_3104b_hour.fecha = flns_3103b_hour.fecha 
              and flns_3104b_hour.fins_id = flns_3103b_hour.fins_id
        join
        flns_3114a_hour
              on flns_3104b_hour.fecha = flns_3114a_hour.fecha 
              and flns_3104b_hour.fins_id = flns_3114a_hour.fins_id
        join
        flns_3106a_hour
              on flns_3106a_hour.fecha = flns_3114a_hour.fecha 
              and flns_3106a_hour.fins_id = flns_3114a_hour.fins_id
        join
        flns_3218a_hour
              on flns_3106a_hour.fecha = flns_3218a_hour.fecha 
              and flns_3106a_hour.fins_id = flns_3218a_hour.fins_id
        join
        flns_3115a_hour
              on flns_3115a_hour.fecha = flns_3218a_hour.fecha 
              and flns_3115a_hour.fins_id = flns_3218a_hour.fins_id
        join
        flns_3108b_hour
              on flns_3115a_hour.fecha = flns_3108b_hour.fecha 
              and flns_3115a_hour.fins_id = flns_3108b_hour.fins_id
        join
        flns_3109b_hour
              on flns_3109b_hour.fecha = flns_3108b_hour.fecha 
              and flns_3109b_hour.fins_id = flns_3108b_hour.fins_id
--where   flns_3105b_hour.FECHA = SMMT.FECHA
--AND SMMT.FECHA = UMLM_MMMT.FECHA
--AND UMLM_MMMT.FECHA = SGSM.FECHA
--AND flns_3105b_hour.FINS_ID = SMMT.FINS_ID
--AND SMMT.FINS_ID = UMLM_MMMT.FINS_ID
--AND UMLM_MMMT.FINS_ID = SGSM.FINS_ID