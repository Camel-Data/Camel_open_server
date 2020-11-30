class ETLConfig:

    # -------------- datetime ------------------
    OPEN_SERVER_DAYS = 10

    OFFSET = 0


    # -------------- user ------------------
    # always include id and create_time
    USER_INFO_FIELDS = ["id","create_time","language","register_channel","login_channel",
                  "country_code", "register_ip_country", "bind_channel", "device_id",
                  "device_model","source_channel"]

    # -------------- engage ------------------
    # always include uid and date_id
    ENGAGE_FIELDS = ['uid','date_id','chat_num','active_time']

    # -------------- payment------------------
    # always include user_id and create_time
    PAYMENT_FIELDS = ['user_id','create_time','currency_ammount']

    # -------------- battle ------------------
    # always include uid and date_id
    BATTLE_FIELDS = ['uid','date_id','pvp','fastpvp','gather_o + gather_r gather']

    # -------------- resource ------------------
    # always include uid and date_id
    RESOURCE_FIELDS = ['uid','date_id','city_level','mojo']

    # -------------- troop ------------------
    # always include uid and date_id
    TROOP_FIELDS = ['uid','date_id','losePower, raisePower',
                    'loseTroop','addTroop','woundTroop','alliance_id']

    # -------------- gold farmer rob ------------------
    GOLD_FARMER_ATTK = ['id','atk_id', 'atk_ip','atk_name', 'atk_device_id',
                 'atk_online', 'atk_user_lvl','atk_city_lvl',
                 'atk_alli_id', 'atk_alli_full', 'atk_alli_short',
                 'atk_server', 'atk_node', 'atk_survive', 'atk_death',
                 'atk_wound','atk_kill', 'atk_power']

    GOLD_FARMER_DEF = ['id','def_id', 'def_ip','def_name','def_device_id',
                 'def_online', 'def_user_lvl', 'def_city_lvl',
                 'def_alli_id', 'def_alli_full', 'def_alli_short',
                 'def_server','def_node', 'def_survive', 'def_death',
                 'def_wound', 'def_kill','def_power']

    GOLD_FARMER_BTL = ['id','date_id', 'create_time', 'food',
               'oil', 'rare_earth', 'iron', 'quick_fight','win' ]

    GOLD_FARMER_FIELDS = GOLD_FARMER_ATTK + GOLD_FARMER_DEF + GOLD_FARMER_BTL
