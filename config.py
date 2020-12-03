class ETLConfig:

    # -------------- datetime ------------------
    OPEN_SERVER_DAYS = 6

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
    PAYMENT_FIELDS = ['user_id','create_time','currency_ammount', 'type']

    # -------------- battle ------------------
    # always include uid and date_id
    BATTLE_FIELDS = ['uid','date_id','pvp','fastpvp','gather_f + gather_o + gather_r  + gather_i +  gather_g + gather_h gather',
                    'ruins', 'challenge','rebel','terri_def','elite_war','elite_advt',
                    'realm_war','cross_rw','void_war','legend_war','chapion_war',
                    'chapion_war_bet','level_1_1', 'level_1_2','level_2_1',
                    'level_2_2', 'level_3_1','level_3_2']

    # -------------- resource ------------------
    # always include uid and date_id
    RESOURCE_FIELDS = ['uid','date_id','city_level','mojo', 'mojo_add', 'mojo_use',
                        'food', 'food_add', 'food_use','oil', 'oil_add', 'oil_use',
                        'rare_earth', 'rare_earth_add','rare_earth_use', 'iron', 'iron_add',
                        'iron_use','high_energe_ore', 'high_energe_ore_add', 'high_energe_ore_use']

    # -------------- troop ------------------
    # always include uid and date_id
    TROOP_FIELDS = ['uid','date_id','losePower', 'raisePower', 'loseTroop', 'woundTroop', 'addTroop',
                    'dead1', 'dead2', 'dead3', 'dead4', 'dead5', 'dead6', 'dead7','dead8', 'dead9', 'dead10',
                    'dead11', 'wound1', 'wound2', 'wound3','wound4', 'wound5', 'wound6', 'wound7', 'wound8', 'wound9',
                    'wound10', 'wound11', 'alliance_id']

    # -------------- gold farmer rob ------------------
    # always include atk_id, def_id, date_id
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

    GOLD_FARMER_FIELDS = list(set(GOLD_FARMER_ATTK + GOLD_FARMER_DEF + GOLD_FARMER_BTL))


    # -------------- gold farmer rob transform ------------------
    # if you need to use the following data fields for aggregation,
    # you should include these fields in GOLD_FAMER_... configurations (above)
    # the `date` filed is added via open_server_goldfarmer function
    # the logic of the following configurations is:
    # attk: group by - > agg with different methods -> merge attk dataframe
    # def:  group by - > agg with different methods -> merge def deframe
    # merge attk and def on [atk_id, date] <--> [def_id, date], method = 'outer'
    # then you get aggregated features of each user, each day

    ATTK_GROUPBY = ['atk_id','date']

    ATTK_LAST = ['atk_ip','atk_name','atk_device_id','atk_user_lvl',
    'atk_city_lvl','atk_alli_id','atk_alli_full', 'atk_alli_short',
    'atk_server','atk_node','atk_power']

    ATTK_SUM = ['atk_online','atk_survive', 'atk_death','atk_wound','atk_kill']

    ATTK_COUNT = ['id']


    DEF_GROUPBY = ['def_id','date']

    DEF_LAST = ['def_ip','def_name','def_device_id','def_user_lvl',
    'def_city_lvl','def_alli_id','def_alli_full', 'def_alli_short',
    'def_server','def_node','def_power']

    DEF_SUM = ['def_online','def_survive', 'def_death','def_wound','def_kill']

    DEF_COUNT = ['id']


config_list = {'ETLConfig':ETLConfig}
