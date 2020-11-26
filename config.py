class ETLConfig:

    OPEN_SERVER_DAYS = 10

    OFFSET = 0

    # always include id and create_time
    USER_INFO_FIELDS = ["id","create_time","language","register_channel","login_channel",
                  "country_code", "register_ip_country", "bind_channel", "device_id",
                  "device_model","source_channel"]


    # always include uid and date_id
    ENGAGE_FIELDS = ['uid','date_id','chat_num','active_time']

    # always include user_id and create_time
    PAYMENT_FIELDS = ['user_id','create_time','currency_ammount']

    # always include uid and date_id
    BATTLE_FIELDS = ['uid','date_id','pvp','fastpvp','gather_o + gather_r gather']

    # always include uid and date_id
    RESOURCE_FIELDS = ['uid','date_id','city_leve','mojo']
