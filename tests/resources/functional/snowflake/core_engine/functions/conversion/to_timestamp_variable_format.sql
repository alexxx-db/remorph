-- snowflake sql:
	SELECT TO_TIMESTAMP(str, fmt) FROM (VALUES ('2024-11-20T18:05:59.123456789', 'YYYY-MM-DD"T"HH24:MI:SS.FF'), ('Thu, 21 Dec 2000 04:01:07 PM +0200', 'DY, DD MON YYYY HH12:MI:SS AM TZHTZM') ) AS vals(str, fmt);

-- databricks sql:
SELECT IF(
	STARTSWITH(fmt, 'DY'),
	TO_TIMESTAMP(SUBSTR(str, 4), SUBSTR(REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                REPLACE(
                                  REPLACE(
                                    REPLACE(
                                      REPLACE(
                                        REPLACE(
                                          REPLACE(
                                            REPLACE(
                                              REPLACE(
                                                REPLACE(
                                                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(fmt, 'YYYY', 'yyyy'), 'YY', 'yy'), 'MON', 'MMM'), 'DD', 'dd'), 'DY', 'EEE'),
                                                  'HH24',
                                                  'HH'
                                                ),
                                                'HH12',
                                                'hh'
                                              ),
                                              'AM',
                                              'a'
                                            ),
                                            'PM',
                                            'a'
                                          ),
                                          'MI',
                                          'mm'
                                        ),
                                        'SS',
                                        'ss'
                                      ),
                                      'FF9',
                                      'SSSSSSSSS'
                                    ),
                                    'FF8',
                                    'SSSSSSSS'
                                  ),
                                  'FF7',
                                  'SSSSSSS'
                                ),
                                'FF6',
                                'SSSSSS'
                              ),
                              'FF5',
                              'SSSSS'
                            ),
                            'FF4',
                            'SSSS'
                          ),
                          'FF3',
                          'SSS'
                        ),
                        'FF2',
                        'SS'
                      ),
                      'FF1',
                      'S'
                    ),
                    'FF0',
                    ''
                  ),
                  'FF',
                  'SSSSSSSSS'
                ),
                'TZH:TZM',
                'ZZZ'
              ),
              'TZHTZM',
              'ZZZ'
            ),
            'TZH',
            'ZZZ'
          ),
          'UUUU',
          'yyyy'
        ), '"', '\''), 4)),
	TO_TIMESTAMP(str, REPLACE(
        REPLACE(
          REPLACE(
            REPLACE(
              REPLACE(
                REPLACE(
                  REPLACE(
                    REPLACE(
                      REPLACE(
                        REPLACE(
                          REPLACE(
                            REPLACE(
                              REPLACE(
                                REPLACE(
                                  REPLACE(
                                    REPLACE(
                                      REPLACE(
                                        REPLACE(
                                          REPLACE(
                                            REPLACE(
                                              REPLACE(
                                                REPLACE(
                                                  REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(fmt, 'YYYY', 'yyyy'), 'YY', 'yy'), 'MON', 'MMM'), 'DD', 'dd'), 'DY', 'EEE'),
                                                  'HH24',
                                                  'HH'
                                                ),
                                                'HH12',
                                                'hh'
                                              ),
                                              'AM',
                                              'a'
                                            ),
                                            'PM',
                                            'a'
                                          ),
                                          'MI',
                                          'mm'
                                        ),
                                        'SS',
                                        'ss'
                                      ),
                                      'FF9',
                                      'SSSSSSSSS'
                                    ),
                                    'FF8',
                                    'SSSSSSSS'
                                  ),
                                  'FF7',
                                  'SSSSSSS'
                                ),
                                'FF6',
                                'SSSSSS'
                              ),
                              'FF5',
                              'SSSSS'
                            ),
                            'FF4',
                            'SSSS'
                          ),
                          'FF3',
                          'SSS'
                        ),
                        'FF2',
                        'SS'
                      ),
                      'FF1',
                      'S'
                    ),
                    'FF0',
                    ''
                  ),
                  'FF',
                  'SSSSSSSSS'
                ),
                'TZH:TZM',
                'ZZZ'
              ),
              'TZHTZM',
              'ZZZ'
            ),
            'TZH',
            'ZZZ'
          ),
          'UUUU',
          'yyyy'
        ), '"', '\''))
<<<<<<< HEAD
<<<<<<< HEAD
) FROM VALUES ('2024-11-20T18:05:59.123456789', 'YYYY-MM-DD"T"HH24:MI:SS.FF'), ('Thu, 21 Dec 2000 04:01:07 PM +0200', 'DY, DD MON YYYY HH12:MI:SS AM TZHTZM') AS vals(str, fmt);
=======
) FROM VALUES ('2024-11-20T18:05:59.123456789', 'YYYY-MM-DD"T"HH24:MI:SS.FF'), ('Thu, 21 Dec 2000 04:01:07 PM +0200', 'DY, DD MON YYYY HH12:MI:SS AM TZHTZM') AS vals(str, fmt);
>>>>>>> 9dcc986e (Fix implementation of Snowflake's TO_TIME/TO_TIMESTAMP functions (#1218))
=======
) FROM VALUES ('2024-11-20T18:05:59.123456789', 'YYYY-MM-DD"T"HH24:MI:SS.FF'), ('Thu, 21 Dec 2000 04:01:07 PM +0200', 'DY, DD MON YYYY HH12:MI:SS AM TZHTZM') AS vals(str, fmt);
>>>>>>> 9ffc6a0d (EditorConfig setup for project (#1246))
