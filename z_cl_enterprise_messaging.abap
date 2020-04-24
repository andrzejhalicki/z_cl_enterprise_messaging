CLASS z_cl_enterprise_messaging DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    TYPES: BEGIN OF ty_queue,
             name                       TYPE string,
             messagecount               TYPE int4,
             queuesizeinbytes           TYPE int4,
             unacknowledgedmessagecount TYPE int4,
             maxqueuesizeinbytes        TYPE int4,
             maxqueuemessagecount       TYPE int4,
           END OF ty_queue,
           tt_queue TYPE TABLE OF ty_queue WITH KEY name.

    METHODS: constructor IMPORTING iv_messaging_destination  TYPE c
                                   iv_management_destination TYPE c
                                   iv_token_destination      TYPE c,
      consume_queue IMPORTING iv_queue           TYPE string
                    RETURNING VALUE(rv_response) TYPE string,
      get_queues RETURNING VALUE(rt_queues) TYPE tt_queue,
      get_queue_statistics IMPORTING iv_queue                   TYPE string
                           RETURNING VALUE(rs_queue_statistics) TYPE string,
      post_to_queue IMPORTING iv_message TYPE string
                              iv_queue   TYPE string,
      post_to_topic IMPORTING iv_message TYPE string
                              iv_topic   TYPE string.

  PROTECTED SECTION.
  PRIVATE SECTION.

    TYPES: BEGIN OF ty_token,
             access_token TYPE string,
             token_type   TYPE string,
             expires_in   TYPE string,
           END OF ty_token.

    DATA: _mo_http_client_token      TYPE REF TO if_http_client,
          _mo_http_client_management TYPE REF TO if_http_client,
          _mo_http_client_messaging  TYPE REF TO if_http_client,
          _mo_rest_token             TYPE REF TO cl_rest_http_client,
          _mo_rest_management        TYPE REF TO cl_rest_http_client,
          _mo_rest_messaging         TYPE REF TO cl_rest_http_client.

    DATA: _ms_token             TYPE ty_token,
          _mv_token_expiry_time TYPE tzntstmpl.

    DATA: _mv_uri_get_queues           TYPE string VALUE '/hub/rest/api/v1/management/messaging/queues',
          _mv_uri_get_queue_statistics TYPE string VALUE '/hub/rest/api/v1/management/messaging/queues/{queue}/statistics',
          _mv_uri_consume_queue        TYPE string VALUE '/messagingrest/v1/queues/{queue}/messages/consumption',
          _mv_uri_post_to_queue        TYPE string VALUE '/messagingrest/v1/queues/{queue}/messages',
          _mv_uri_post_to_topic        TYPE string VALUE '/messagingrest/v1/topics/{topic}/messages'.

    METHODS: _create_by_destination IMPORTING iv_destination TYPE c
                                    CHANGING  co_http_client TYPE REF TO if_http_client
                                              co_rest_client TYPE REF TO cl_rest_http_client,
      _check_token_still_valid   RETURNING VALUE(rv_valid) TYPE abap_bool,
      _get_token,
      _get_management_response_str IMPORTING iv_uri             TYPE string
                                   RETURNING VALUE(rv_response) TYPE string,
      _get_messaging_response_str IMPORTING iv_uri             TYPE string
                                  RETURNING VALUE(rv_response) TYPE string.

ENDCLASS.

CLASS z_cl_enterprise_messaging IMPLEMENTATION.

  METHOD constructor.

    me->_create_by_destination(
      EXPORTING
        iv_destination = iv_management_destination
      CHANGING
        co_http_client = _mo_http_client_management
        co_rest_client = _mo_rest_management
    ).

    me->_create_by_destination(
      EXPORTING
        iv_destination = iv_token_destination
      CHANGING
        co_http_client = _mo_http_client_token
        co_rest_client = _mo_rest_token
    ).

    me->_create_by_destination(
      EXPORTING
        iv_destination = iv_messaging_destination
      CHANGING
        co_http_client = _mo_http_client_messaging
        co_rest_client = _mo_rest_messaging
    ).

    me->_get_token( ).

  ENDMETHOD.

  METHOD _create_by_destination.

    cl_http_client=>create_by_destination(
       EXPORTING
           destination              = iv_destination
       IMPORTING
           client                   = co_http_client
       EXCEPTIONS
           argument_not_found       = 1
           destination_not_found    = 2
           destination_no_authority = 3
           plugin_not_active        = 4
           internal_error           = 5
           OTHERS                   = 6
   ).
    IF sy-subrc <> 0.
      MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
        WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4.
    ENDIF.

    co_rest_client = NEW #( co_http_client ).

  ENDMETHOD.

  METHOD _get_token.

    CHECK NOT me->_check_token_still_valid( ).

    _mo_rest_token->if_rest_client~post( _mo_rest_token->if_rest_client~create_request_entity( ) ).

    DATA(lv_response) = _mo_rest_token->if_rest_client~get_response_entity( )->get_string_data( ).

    /ui2/cl_json=>deserialize(
      EXPORTING
        json             = lv_response
      CHANGING
        data             = _ms_token
    ).

    GET TIME STAMP FIELD DATA(lv_timestamp).

    _mv_token_expiry_time = cl_abap_tstmp=>add( EXPORTING tstmp   = lv_timestamp
                                                          secs    = CONV #( _ms_token-expires_in ) ).

  ENDMETHOD.

  METHOD _check_token_still_valid.

    GET TIME STAMP FIELD DATA(lv_timestamp).

    rv_valid = COND #( WHEN lv_timestamp >= _mv_token_expiry_time THEN abap_false
                       ELSE abap_true ).

  ENDMETHOD.

  METHOD consume_queue.

    DATA(lv_uri) = replace( val = _mv_uri_consume_queue sub = '{queue}' with = iv_queue ).

    rv_response = me->_get_messaging_response_str( lv_uri ).

  ENDMETHOD.

  METHOD get_queues.

    DATA(lv_response) = me->_get_management_response_str( _mv_uri_get_queues ).

    /ui2/cl_json=>deserialize(
      EXPORTING
        json             = lv_response
      CHANGING
        data             = rt_queues
    ).

  ENDMETHOD.

  METHOD get_queue_statistics.

    DATA(lv_uri) = replace( val = _mv_uri_get_queue_statistics sub = '{queue}' with = iv_queue ).

    rs_queue_statistics = me->_get_management_response_str( lv_uri ).

  ENDMETHOD.

  METHOD post_to_queue.

    DATA(lv_uri) = replace( val = _mv_uri_post_to_queue sub = '{queue}' with = iv_queue ).

    DATA(lv_response) = me->_get_messaging_response_str( lv_uri ).

  ENDMETHOD.

  METHOD post_to_topic.

    DATA(lv_uri) = replace( val = _mv_uri_post_to_topic sub = '{topic}' with = iv_topic ).

    DATA(lv_response) = me->_get_messaging_response_str( lv_uri ).

  ENDMETHOD.

  METHOD _get_management_response_str.

    me->_get_token( ).

    cl_http_utility=>set_request_uri(
        EXPORTING
            request = _mo_http_client_management->request
            uri     = iv_uri
    ).

    DATA(lo_request) = _mo_rest_management->if_rest_client~create_request_entity( ).

    lo_request->set_header_field(
      EXPORTING
        iv_name  = 'x-qos'
        iv_value = '0'
    ).

    lo_request->set_header_field(
      EXPORTING
        iv_name  = 'Authorization'
        iv_value = |{ _ms_token-token_type } { _ms_token-access_token }|
    ).

    _mo_rest_management->if_rest_client~get( ).

    rv_response = _mo_rest_management->if_rest_client~get_response_entity( )->get_string_data( ).

  ENDMETHOD.

  METHOD _get_messaging_response_str.

    me->_get_token( ).

    cl_http_utility=>set_request_uri(
        EXPORTING
            request = _mo_http_client_messaging->request
            uri     = iv_uri
    ).

    DATA(lo_request) = _mo_rest_messaging->if_rest_client~create_request_entity( ).

    lo_request->set_header_field(
      EXPORTING
        iv_name  = 'x-qos'
        iv_value = '0'
    ).

    lo_request->set_header_field(
      EXPORTING
        iv_name  = 'Authorization'
        iv_value = |{ _ms_token-token_type } { _ms_token-access_token }|
    ).

    _mo_rest_messaging->if_rest_client~post( io_entity = lo_request ).

    rv_response = _mo_rest_messaging->if_rest_client~get_response_entity( )->get_string_data( ).

  ENDMETHOD.

ENDCLASS.
