# -*- mode: python -*-

Import("env has_option")
Import("get_option")
Import("http_client")

import libdeps

env = env.Clone()

if not has_option("ssl"):
    env.FatalError("SSL not enabled. Enterprise MongoDB must be built with --ssl specified")

env.InjectMongoIncludePaths()
env.InjectModule("enterprise", builder=True, consumer=False)

conf = Configure(env, help=False)

if not conf.CheckLibWithHeader(
    "sasl2",
    ["stddef.h","sasl/sasl.h"], "C",
    "sasl_version_info(0, 0, 0, 0, 0, 0);",
    autoadd=False):
    env.ConfError("Could not find <sasl/sasl.h> and sasl library, required for "
        "enterprise build.")

if http_client != "on":
    env.ConfError("Enterprise build requires http client support")

if not env.TargetOSIs("windows"):
    if not conf.CheckLibWithHeader(
            "ldap",
            ["ldap.h"], "C",
            "ldap_is_ldap_url(\"ldap://127.0.0.1\");", autoadd=False):
        env.ConfError("Could not find <ldap.h> and ldap library from OpenLDAP, "
                       "required for LDAP authorization in the enterprise build")
    if not conf.CheckLibWithHeader(
            "lber",
            ["lber.h"], "C",
            "ber_free(NULL, 0);", autoadd=False):
        env.ConfError("Could not find <lber.h> and lber library from OpenLDAP, "
                      "required for LDAP authorizaton in the enterprise build")
    conf.env['MONGO_LDAP_LIB'] = ["ldap", "lber"]
else:
    conf.env['MONGO_LDAP_LIB'] = ["Wldap32"]

if conf.CheckLib(library="gssapi_krb5", autoadd=False):
    conf.env['MONGO_GSSAPI_IMPL'] = "gssapi"
    conf.env['MONGO_GSSAPI_LIB'] = "gssapi_krb5"
    if env.TargetOSIs("freebsd"):
        conf.env['MONGO_GSSAPI_LIB'] = ["gssapi", "gssapi_krb5"]
elif env.TargetOSIs("windows"):
    conf.env['MONGO_GSSAPI_IMPL'] = "sspi"
    conf.env['MONGO_GSSAPI_LIB'] = "secur32"
else:
    env.ConfError("Could not find gssapi_krb5 library nor Windows OS, required for "
                  "enterprise build.")

env = conf.Finish()

env.SConscript(
    dirs=[
        'src/encryptdb',
        'src/inmemory',
        'src/ldap',
        'src/rlp',
        'src/queryable',
        'src/watchdog',
    ],
    exports=[
        'env',
    ],
)

env.Library('audit_enterprise',
            ['src/audit/audit_application_message.cpp',
             'src/audit/audit_authentication.cpp',
             'src/audit/audit_authz_check.cpp',
             'src/audit/audit_event.cpp',
             'src/audit/audit_indexes_collections_databases.cpp',
             'src/audit/audit_log_domain.cpp',
             'src/audit/audit_manager_global.cpp',
             'src/audit/audit_options.cpp',
             'src/audit/audit_private.cpp',
             'src/audit/audit_replset.cpp',
             'src/audit/audit_role_management.cpp',
             'src/audit/audit_sharding.cpp',
             'src/audit/audit_shutdown.cpp',
             'src/audit/audit_user_management.cpp',
             ],
            LIBDEPS=[
                '$BUILD_DIR/mongo/base',
                '$BUILD_DIR/mongo/db/auth/auth',
                '$BUILD_DIR/mongo/util/net/network',
            ],
            LIBDEPS_PRIVATE=[
                '$BUILD_DIR/mongo/bson/mutable/mutable_bson',
                '$BUILD_DIR/mongo/db/auth/authprivilege',
                '$BUILD_DIR/mongo/db/matcher/expressions',
                '$BUILD_DIR/mongo/db/server_options_core',
                '$BUILD_DIR/mongo/util/options_parser/options_parser'
            ],
            LIBDEPS_DEPENDENTS=[
                ('$BUILD_DIR/mongo/db/audit', libdeps.dependency.Public),
            ],
)

env.Library(
    target='audit_command',
    source=[
        'src/audit/audit_command.cpp',
    ],
    LIBDEPS=[
        '$BUILD_DIR/mongo/db/auth/auth',
        '$BUILD_DIR/mongo/db/audit',
        '$BUILD_DIR/mongo/db/commands',
        '$BUILD_DIR/mongo/db/service_context',
    ],
    LIBDEPS_PRIVATE=[
        '$BUILD_DIR/mongo/db/auth/authprivilege',
    ],
    LIBDEPS_DEPENDENTS=[
        '$BUILD_DIR/mongo/db/commands/mongod',
    ],
)

# The auditing code needs to be built into the "coredb" library because there is code in there that
# references audit functions.  However, the "coredb" library is also currently shared by server
# programs, such as mongod and mongos, as well as client programs, such as mongodump and
# mongoexport.  For these reasons, we have no choice but to build the audit module into all of
# these, even though it's dead code in the client programs.  To avoid actually allowing the user to
# configure this dead code, we need to separate the option registration into this
# "audit_configuration" library and add it to only mongod and mongos.  Because audit defaults to
# disabled this effectively prevents this code from being run in client programs.
env.Library('audit_configuration',
            'src/audit/audit_options_init.cpp',
            LIBDEPS=['audit_enterprise'],
            LIBDEPS_PRIVATE=[
                '$BUILD_DIR/mongo/util/options_parser/options_parser',
            ],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongodmain'],
            PROGDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongos'])

if not env.TargetOSIs("darwin"):
    env.Library('mongosnmp',
                ['src/snmp/serverstatus_client.cpp',
                 'src/snmp/snmp.cpp',
                 'src/snmp/snmp_oid.cpp',
                 'src/snmp/snmp_options.cpp'
                ],
                LIBDEPS=[
                    '$BUILD_DIR/mongo/base',
                    '$BUILD_DIR/mongo/client/clientdriver_network',
                    '$BUILD_DIR/mongo/db/bson/dotted_path_support',
                    '$BUILD_DIR/mongo/db/dbdirectclient',
                    '$BUILD_DIR/mongo/db/initialize_snmp',
                    '$BUILD_DIR/mongo/db/repl/repl_coordinator_interface',
                    '$BUILD_DIR/mongo/db/repl/repl_settings',
                    '$BUILD_DIR/mongo/util/processinfo',
                    'src/watchdog/watchdog_mongod',
                ],
                LIBDEPS_PRIVATE=[
                    '$BUILD_DIR/mongo/util/options_parser/options_parser',
                ],
                LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongodmain'],
                SYSLIBDEPS=env.get('SNMP_SYSLIBDEPS', []),
    )

env.Library('mongosaslserversession',
            ['src/sasl/canon_mongodb_internal.cpp',
             'src/sasl/mongo_${MONGO_GSSAPI_IMPL}.cpp',
             ],
            LIBDEPS=[
                '$BUILD_DIR/mongo/db/auth/saslauth',
                '$BUILD_DIR/mongo/db/server_parameters',
            ],
            SYSLIBDEPS=['sasl2'] + [env['MONGO_GSSAPI_LIB']],
)

env.Library('auth_delay',
            'src/sasl/auth_delay.cpp',
            LIBDEPS=['$BUILD_DIR/mongo/db/auth/sasl_options'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongodmain'],
            PROGDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongos'],
)

env.Library('mongosaslservercommon',
            [
             'src/sasl/cyrus_sasl_authentication_session.cpp',
             'src/sasl/ldap_sasl_authentication_session.cpp'
            ],
            LIBDEPS=['src/ldap/ldap_manager',
                     'src/ldap/ldap_name_map',
                     'mongosaslserversession'],
            LIBDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongodmain'],
            PROGDEPS_DEPENDENTS=['$BUILD_DIR/mongo/mongos'],
)

if env.TargetOSIs("windows"):
    # Ensure we're building with /MD or /MDd
    mdFlags = ["/MD","/MDd"]
    hasFlag = 0
    for mdFlag in mdFlags:
        if mdFlag in env['CCFLAGS']:
            hasFlag += 1
    if hasFlag != 1:
        env.FatalError("You must enable dynamic CRT --dynamic-windows to build the "
            "enterprise version")

    sspi_test = env.Program('sasl_authentication_session_sspi_test',
                            ['src/sasl/sasl_authentication_session_sspi_test.cpp'],
                            LIBDEPS=[
                                'mongosaslserversession',
                                '$BUILD_DIR/mongo/db/auth/authmocks',
                            ])
# SERVER-10700
#    env.RegisterUnitTest(sspi_test[0])
else:
    gssapi_test = env.Program('sasl_authentication_session_gssapi_test',
                              ['src/sasl/sasl_authentication_session_gssapi_test.cpp'],
                              LIBDEPS=['mongosaslserversession',
                                       'mongosaslservercommon',
                                       'src/ldap/ldap_manager_init',
                                       '$BUILD_DIR/mongo/base',
                                       '$BUILD_DIR/mongo/util/net/network',
                                       '$BUILD_DIR/mongo/db/auth/auth',
                                       '$BUILD_DIR/mongo/db/auth/authmocks',
                                       '$BUILD_DIR/mongo/db/auth/saslauth',
                                       '$BUILD_DIR/mongo/client/clientdriver_network',
                                       '$BUILD_DIR/mongo/client/sasl_client',
                                       '$BUILD_DIR/mongo/db/service_context_test_fixture',
                                       '$BUILD_DIR/mongo/executor/thread_pool_task_executor',
                                       '$BUILD_DIR/mongo/executor/network_interface_thread_pool',
                                       '$BUILD_DIR/mongo/executor/network_interface_factory',
                                       '$BUILD_DIR/mongo/unittest/unittest'])
    env.RegisterUnitTest(gssapi_test[0])

mongodecrypt = env.Program(
    target="mongodecrypt",
    source=[
        "src/encryptdb/decrypt_tool.cpp",
        "src/encryptdb/decrypt_tool_options.cpp",
    ] + env.WindowsResourceFile("src/encryptdb/decrypt_tool.rc"),
    LIBDEPS=[
        "$BUILD_DIR/mongo/base",
        "$BUILD_DIR/mongo/db/log_process_details",
        "$BUILD_DIR/mongo/util/options_parser/options_parser_init",
        "$BUILD_DIR/mongo/util/signal_handlers",
        "$BUILD_DIR/mongo/util/version_impl",
        "src/encryptdb/key_acquisition",
        "src/encryptdb/symmetric_crypto",
    ],
)

env.Alias("all", mongodecrypt)

hygienic = get_option('install-mode') == 'hygienic'
if not hygienic:
    env.Install("#/", mongodecrypt)

mongoldap = env.Program(
    target="mongoldap",
    source=[
        "src/ldap/ldap_tool.cpp",
        "src/ldap/ldap_tool_options.cpp",
    ] +  env.WindowsResourceFile("src/ldap/ldap_tool.rc"),
    LIBDEPS=[
        "$BUILD_DIR/mongo/base",
        "$BUILD_DIR/mongo/base/secure_allocator",
        "$BUILD_DIR/mongo/db/log_process_details",
        "$BUILD_DIR/mongo/util/options_parser/options_parser_init",
        "$BUILD_DIR/mongo/util/signal_handlers",
        "$BUILD_DIR/mongo/util/version_impl",
        "src/ldap/ldap_manager",
        "src/ldap/ldap_options_mongod",
    ],
)

env.Alias("all", mongoldap)

if not hygienic:
    env.Install("#/", mongoldap)
