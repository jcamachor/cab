-- ---------------------------------------------------------------------------------------------------------------------
-- Load data (15min)
-- Turn wal_ minimal, archive mode off .. senders 0

begin;
create table TimeExplosion
(
    time    timestamp,
    queryid bigint
);
copy TimeExplosion from '/tmp/ts-explosion.csv' CSV HEADER;
commit;

-- ---------------------------------------------------------------------------------------------------------------------
-- Sum up to hours (30min)

begin;
create table MinuteTimeExplosion
(
    queryid  bigint,
    time     timestamp,
    fraction float
);

with SamplesPerQuery as (select queryid, count(*) as totalSamples
                         from TimeExplosion
                         where date_trunc('day', time) = '2018-02-22'::date
                         group by queryid)
insert
into MinuteTimeExplosion (select t.queryid, date_trunc('minute', t.time), count(*) * 1.0 / max(s.totalSamples)
                          from TimeExplosion t,
                               SamplesPerQuery s
                          where t.queryid = s.queryid
                            and date_trunc('day', t.time) = '2018-02-22'::date
                          group by t.queryid, date_trunc('minute', t.time));
commit;

-- ---------------------------------------------------------------------------------------------------------------------
-- Join with day (1min)

begin;
create table day_te_minute
(
    queryid                         bigint    not null,
    warehouseid                     bigint    not null,
    databaseid                      bigint,
    createdtime                     timestamp not null,
    endtime                         timestamp not null,
    durationtotal                   bigint    not null,
    durationexec                    bigint    not null,
    durationcontrolplane            bigint    not null,
    durationcompiling               bigint    not null,
    compilationtime                 bigint    not null,
    scheduletime                    bigint    not null,
    servercount                     bigint    not null,
    exectime                        bigint    not null,
    warehousesize                   bigint    not null,
    perservercores                  bigint    not null,
    persistentreadbytess3           bigint    not null,
    persistentreadrequestss3        bigint    not null,
    persistentreadbytescache        bigint    not null,
    persistentreadrequestscache     bigint    not null,
    persistentwritebytescache       bigint    not null,
    persistentwriterequestscache    bigint    not null,
    persistentwritebytess3          bigint    not null,
    persistentwriterequestss3       bigint    not null,
    intdatawritebyteslocalssd       bigint    not null,
    intdatawriterequestslocalssd    bigint    not null,
    intdatareadbyteslocalssd        bigint    not null,
    intdatareadrequestslocalssd     bigint    not null,
    intdatawritebytess3             bigint    not null,
    intdatawriterequestss3          bigint    not null,
    intdatareadbytess3              bigint    not null,
    intdatareadrequestss3           bigint    not null,
    intdatawritebytesuncompressed   bigint    not null,
    ioremoteexternalreadbytes       bigint    not null,
    ioremoteexternalreadrequests    bigint    not null,
    intdatanetreceivedbytes         bigint    not null,
    intdatanetsentbytes             bigint    not null,
    intdatanetsentrequests          bigint    not null,
    intdatanetsentbytesuncompressed bigint    not null,
    producedrows                    bigint    not null,
    returnedrows                    bigint    not null,
    filestolencount                 bigint    not null,
    remoteseqscanfileops            bigint    not null,
    localseqscanfileops             bigint    not null,
    localwritefileops               bigint    not null,
    remoteskipscanfileops           bigint    not null,
    remotewritefileops              bigint    not null,
    filescreated                    bigint    not null,
    scanassignedbytes               bigint    not null,
    scanassignedfiles               bigint    not null,
    scanbytes                       bigint    not null,
    scanfiles                       bigint    not null,
    scanoriginalfiles               bigint    not null,
    usercputime                     bigint    not null,
    systemcputime                   bigint    not null,
    profidle                        bigint    not null,
    profcpu                         bigint    not null,
    profpersistentreadcache         bigint    not null,
    profpersistentwritecache        bigint    not null,
    profpersistentreads3            bigint    not null,
    profpersistentwrites3           bigint    not null,
    profintdatareadlocalssd         bigint    not null,
    profintdatawritelocalssd        bigint    not null,
    profintdatareads3               bigint    not null,
    profintdatawrites3              bigint    not null,
    profremoteextread               bigint    not null,
    profremoteextwrite              bigint    not null,
    profreswrites3                  bigint    not null,
    proffsmeta                      bigint    not null,
    profdataexchangenet             bigint    not null,
    profdataexchangemsg             bigint    not null,
    profcontrolplanemsg             bigint    not null,
    profos                          bigint    not null,
    profmutex                       bigint    not null,
    profsetup                       bigint    not null,
    profsetupmesh                   bigint    not null,
    profteardown                    bigint    not null,
    profscanrso                     bigint    not null,
    profxtscanrso                   bigint    not null,
    profprojrso                     bigint    not null,
    profsortrso                     bigint    not null,
    proffilterrso                   bigint    not null,
    profresrso                      bigint    not null,
    profdmlrso                      bigint    not null,
    profhjrso                       bigint    not null,
    profbufrso                      bigint    not null,
    profflatrso                     bigint    not null,
    profbloomrso                    bigint    not null,
    profaggrso                      bigint    not null,
    profbandrso                     bigint    not null,
    profpercentilerso               bigint    not null,
    profudtfrso                     bigint    not null,
    memoryused                      bigint    not null,
    timeslice                       timestamp not null,
    timefraction                    float     not null
);

insert into day_te_minute (
    select d.queryid,
           d.warehouseid,
           d.databaseid,
           d.createdtime,
           d.endtime,
           d.durationtotal,
           d.durationexec,
           d.durationcontrolplane,
           d.durationcompiling,
           d.compilationtime,
           d.scheduletime,
           d.servercount,
           d.exectime,
           d.warehousesize,
           d.perservercores,
           round(d.persistentreadbytess3 * h.fraction)           as persistentreadbytess3,
           round(d.persistentreadrequestss3 * h.fraction)        as persistentreadrequestss3,
           round(d.persistentreadbytescache * h.fraction)        as persistentreadbytescache,
           round(d.persistentreadrequestscache * h.fraction)     as persistentreadrequestscache,
           round(d.persistentwritebytescache * h.fraction)       as persistentwritebytescache,
           round(d.persistentwriterequestscache * h.fraction)    as persistentwriterequestscache,
           round(d.persistentwritebytess3 * h.fraction)          as persistentwritebytess3,
           round(d.persistentwriterequestss3 * h.fraction)       as persistentwriterequestss3,
           round(d.intdatawritebyteslocalssd * h.fraction)       as intdatawritebyteslocalssd,
           round(d.intdatawriterequestslocalssd * h.fraction)    as intdatawriterequestslocalssd,
           round(d.intdatareadbyteslocalssd * h.fraction)        as intdatareadbyteslocalssd,
           round(d.intdatareadrequestslocalssd * h.fraction)     as intdatareadrequestslocalssd,
           round(d.intdatawritebytess3 * h.fraction)             as intdatawritebytess3,
           round(d.intdatawriterequestss3 * h.fraction)          as intdatawriterequestss3,
           round(d.intdatareadbytess3 * h.fraction)              as intdatareadbytess3,
           round(d.intdatareadrequestss3 * h.fraction)           as intdatareadrequestss3,
           round(d.intdatawritebytesuncompressed * h.fraction)   as intdatawritebytesuncompressed,
           round(d.ioremoteexternalreadbytes * h.fraction)       as ioremoteexternalreadbytes,
           round(d.ioremoteexternalreadrequests * h.fraction)    as ioremoteexternalreadrequests,
           round(d.intdatanetreceivedbytes * h.fraction)         as intdatanetreceivedbytes,
           round(d.intdatanetsentbytes * h.fraction)             as intdatanetsentbytes,
           round(d.intdatanetsentrequests * h.fraction)          as intdatanetsentrequests,
           round(d.intdatanetsentbytesuncompressed * h.fraction) as intdatanetsentbytesuncompressed,
           d.producedrows,
           d.returnedrows,
           round(d.filestolencount * h.fraction)                 as filestolencount,
           round(d.remoteseqscanfileops * h.fraction)            as remoteseqscanfileops,
           round(d.localseqscanfileops * h.fraction)             as localseqscanfileops,
           round(d.localwritefileops * h.fraction)               as localwritefileops,
           round(d.remoteskipscanfileops * h.fraction)           as remoteskipscanfileops,
           round(d.remotewritefileops * h.fraction)              as remotewritefileops,
           round(d.filescreated * h.fraction)                    as filescreated,
           round(d.scanassignedbytes * h.fraction)               as scanassignedbytes,
           round(d.scanassignedfiles * h.fraction)               as scanassignedfiles,
           round(d.scanbytes * h.fraction)                       as scanbytes,
           round(d.scanfiles * h.fraction)                       as scanfiles,
           round(d.scanoriginalfiles * h.fraction)               as scanoriginalfiles,
           round(d.usercputime * h.fraction)                     as usercputime,
           round(d.systemcputime * h.fraction)                   as systemcputime,
           round(d.profidle * h.fraction)                        as profidle,
           round(d.profcpu * h.fraction)                         as profcpu,
           round(d.profpersistentreadcache * h.fraction)         as profpersistentreadcache,
           round(d.profpersistentwritecache * h.fraction)        as profpersistentwritecache,
           round(d.profpersistentreads3 * h.fraction)            as profpersistentreads3,
           round(d.profpersistentwrites3 * h.fraction)           as profpersistentwrites3,
           round(d.profintdatareadlocalssd * h.fraction)         as profintdatareadlocalssd,
           round(d.profintdatawritelocalssd * h.fraction)        as profintdatawritelocalssd,
           round(d.profintdatareads3 * h.fraction)               as profintdatareads3,
           round(d.profintdatawrites3 * h.fraction)              as profintdatawrites3,
           round(d.profremoteextread * h.fraction)               as profremoteextread,
           round(d.profremoteextwrite * h.fraction)              as profremoteextwrite,
           round(d.profreswrites3 * h.fraction)                  as profreswrites3,
           round(d.proffsmeta * h.fraction)                      as proffsmeta,
           round(d.profdataexchangenet * h.fraction)             as profdataexchangenet,
           round(d.profdataexchangemsg * h.fraction)             as profdataexchangemsg,
           round(d.profcontrolplanemsg * h.fraction)             as profcontrolplanemsg,
           round(d.profos * h.fraction)                          as profos,
           round(d.profmutex * h.fraction)                       as profmutex,
           round(d.profsetup * h.fraction)                       as profsetup,
           round(d.profsetupmesh * h.fraction)                   as profsetupmesh,
           round(d.profteardown * h.fraction)                    as profteardown,
           round(d.profscanrso * h.fraction)                     as profscanrso,
           round(d.profxtscanrso * h.fraction)                   as profxtscanrso,
           round(d.profprojrso * h.fraction)                     as profprojrso,
           round(d.profsortrso * h.fraction)                     as profsortrso,
           round(d.proffilterrso * h.fraction)                   as proffilterrso,
           round(d.profresrso * h.fraction)                      as profresrso,
           round(d.profdmlrso * h.fraction)                      as profdmlrso,
           round(d.profhjrso * h.fraction)                       as profhjrso,
           round(d.profbufrso * h.fraction)                      as profbufrso,
           round(d.profflatrso * h.fraction)                     as profflatrso,
           round(d.profbloomrso * h.fraction)                    as profbloomrso,
           round(d.profaggrso * h.fraction)                      as profaggrso,
           round(d.profbandrso * h.fraction)                     as profbandrso,
           round(d.profpercentilerso * h.fraction)               as profpercentilerso,
           round(d.profudtfrso * h.fraction)                     as profudtfrso,
           d.memoryused,
           h.time                                                as timeslice,
           h.fraction                                            as timefraction
    from day d,
         MinuteTimeExplosion h
    where d.queryid = h.queryid
      and date_trunc('day', h.time) = '2018-02-22'::date
);
commit;

-- ---------------------------------------------------------------------------------------------------------------------
-- Join with queryies (30min)

begin;
create table Query_Te
(
    queryid                         bigint    not null,
    warehouseid                     bigint    not null,
    databaseid                      bigint,
    createdtime                     timestamp not null,
    endtime                         timestamp not null,
    durationtotal                   bigint    not null,
    durationexec                    bigint    not null,
    durationcontrolplane            bigint    not null,
    durationcompiling               bigint    not null,
    compilationtime                 bigint    not null,
    scheduletime                    bigint    not null,
    servercount                     bigint    not null,
    exectime                        bigint    not null,
    warehousesize                   bigint    not null,
    perservercores                  bigint    not null,
    persistentreadbytess3           bigint    not null,
    persistentreadrequestss3        bigint    not null,
    persistentreadbytescache        bigint    not null,
    persistentreadrequestscache     bigint    not null,
    persistentwritebytescache       bigint    not null,
    persistentwriterequestscache    bigint    not null,
    persistentwritebytess3          bigint    not null,
    persistentwriterequestss3       bigint    not null,
    intdatawritebyteslocalssd       bigint    not null,
    intdatawriterequestslocalssd    bigint    not null,
    intdatareadbyteslocalssd        bigint    not null,
    intdatareadrequestslocalssd     bigint    not null,
    intdatawritebytess3             bigint    not null,
    intdatawriterequestss3          bigint    not null,
    intdatareadbytess3              bigint    not null,
    intdatareadrequestss3           bigint    not null,
    intdatawritebytesuncompressed   bigint    not null,
    ioremoteexternalreadbytes       bigint    not null,
    ioremoteexternalreadrequests    bigint    not null,
    intdatanetreceivedbytes         bigint    not null,
    intdatanetsentbytes             bigint    not null,
    intdatanetsentrequests          bigint    not null,
    intdatanetsentbytesuncompressed bigint    not null,
    producedrows                    bigint    not null,
    returnedrows                    bigint    not null,
    filestolencount                 bigint    not null,
    remoteseqscanfileops            bigint    not null,
    localseqscanfileops             bigint    not null,
    localwritefileops               bigint    not null,
    remoteskipscanfileops           bigint    not null,
    remotewritefileops              bigint    not null,
    filescreated                    bigint    not null,
    scanassignedbytes               bigint    not null,
    scanassignedfiles               bigint    not null,
    scanbytes                       bigint    not null,
    scanfiles                       bigint    not null,
    scanoriginalfiles               bigint    not null,
    usercputime                     bigint    not null,
    systemcputime                   bigint    not null,
    profidle                        bigint    not null,
    profcpu                         bigint    not null,
    profpersistentreadcache         bigint    not null,
    profpersistentwritecache        bigint    not null,
    profpersistentreads3            bigint    not null,
    profpersistentwrites3           bigint    not null,
    profintdatareadlocalssd         bigint    not null,
    profintdatawritelocalssd        bigint    not null,
    profintdatareads3               bigint    not null,
    profintdatawrites3              bigint    not null,
    profremoteextread               bigint    not null,
    profremoteextwrite              bigint    not null,
    profreswrites3                  bigint    not null,
    proffsmeta                      bigint    not null,
    profdataexchangenet             bigint    not null,
    profdataexchangemsg             bigint    not null,
    profcontrolplanemsg             bigint    not null,
    profos                          bigint    not null,
    profmutex                       bigint    not null,
    profsetup                       bigint    not null,
    profsetupmesh                   bigint    not null,
    profteardown                    bigint    not null,
    profscanrso                     bigint    not null,
    profxtscanrso                   bigint    not null,
    profprojrso                     bigint    not null,
    profsortrso                     bigint    not null,
    proffilterrso                   bigint    not null,
    profresrso                      bigint    not null,
    profdmlrso                      bigint    not null,
    profhjrso                       bigint    not null,
    profbufrso                      bigint    not null,
    profflatrso                     bigint    not null,
    profbloomrso                    bigint    not null,
    profaggrso                      bigint    not null,
    profbandrso                     bigint    not null,
    profpercentilerso               bigint    not null,
    profudtfrso                     bigint    not null,
    memoryused                      bigint    not null,
    timeslice                       timestamp not null,
    timefraction                    float     not null
);

insert into Query_Te (
    select q.queryid,
           q.warehouseid,
           q.databaseid,
           q.createdtime,
           q.endtime,
           q.durationtotal,
           q.durationexec,
           q.durationcontrolplane,
           q.durationcompiling,
           q.compilationtime,
           q.scheduletime,
           q.servercount,
           q.exectime,
           q.warehousesize,
           q.perservercores,
           round(q.persistentreadbytess3 * h.fraction)           as persistentreadbytess3,
           round(q.persistentreadrequestss3 * h.fraction)        as persistentreadrequestss3,
           round(q.persistentreadbytescache * h.fraction)        as persistentreadbytescache,
           round(q.persistentreadrequestscache * h.fraction)     as persistentreadrequestscache,
           round(q.persistentwritebytescache * h.fraction)       as persistentwritebytescache,
           round(q.persistentwriterequestscache * h.fraction)    as persistentwriterequestscache,
           round(q.persistentwritebytess3 * h.fraction)          as persistentwritebytess3,
           round(q.persistentwriterequestss3 * h.fraction)       as persistentwriterequestss3,
           round(q.intdatawritebyteslocalssd * h.fraction)       as intdatawritebyteslocalssd,
           round(q.intdatawriterequestslocalssd * h.fraction)    as intdatawriterequestslocalssd,
           round(q.intdatareadbyteslocalssd * h.fraction)        as intdatareadbyteslocalssd,
           round(q.intdatareadrequestslocalssd * h.fraction)     as intdatareadrequestslocalssd,
           round(q.intdatawritebytess3 * h.fraction)             as intdatawritebytess3,
           round(q.intdatawriterequestss3 * h.fraction)          as intdatawriterequestss3,
           round(q.intdatareadbytess3 * h.fraction)              as intdatareadbytess3,
           round(q.intdatareadrequestss3 * h.fraction)           as intdatareadrequestss3,
           round(q.intdatawritebytesuncompressed * h.fraction)   as intdatawritebytesuncompressed,
           round(q.ioremoteexternalreadbytes * h.fraction)       as ioremoteexternalreadbytes,
           round(q.ioremoteexternalreadrequests * h.fraction)    as ioremoteexternalreadrequests,
           round(q.intdatanetreceivedbytes * h.fraction)         as intdatanetreceivedbytes,
           round(q.intdatanetsentbytes * h.fraction)             as intdatanetsentbytes,
           round(q.intdatanetsentrequests * h.fraction)          as intdatanetsentrequests,
           round(q.intdatanetsentbytesuncompressed * h.fraction) as intdatanetsentbytesuncompressed,
           q.producedrows,
           q.returnedrows,
           round(q.filestolencount * h.fraction)                 as filestolencount,
           round(q.remoteseqscanfileops * h.fraction)            as remoteseqscanfileops,
           round(q.localseqscanfileops * h.fraction)             as localseqscanfileops,
           round(q.localwritefileops * h.fraction)               as localwritefileops,
           round(q.remoteskipscanfileops * h.fraction)           as remoteskipscanfileops,
           round(q.remotewritefileops * h.fraction)              as remotewritefileops,
           round(q.filescreated * h.fraction)                    as filescreated,
           round(q.scanassignedbytes * h.fraction)               as scanassignedbytes,
           round(q.scanassignedfiles * h.fraction)               as scanassignedfiles,
           round(q.scanbytes * h.fraction)                       as scanbytes,
           round(q.scanfiles * h.fraction)                       as scanfiles,
           round(q.scanoriginalfiles * h.fraction)               as scanoriginalfiles,
           round(q.usercputime * h.fraction)                     as usercputime,
           round(q.systemcputime * h.fraction)                   as systemcputime,
           round(q.profidle * h.fraction)                        as profidle,
           round(q.profcpu * h.fraction)                         as profcpu,
           round(q.profpersistentreadcache * h.fraction)         as profpersistentreadcache,
           round(q.profpersistentwritecache * h.fraction)        as profpersistentwritecache,
           round(q.profpersistentreads3 * h.fraction)            as profpersistentreads3,
           round(q.profpersistentwrites3 * h.fraction)           as profpersistentwrites3,
           round(q.profintdatareadlocalssd * h.fraction)         as profintdatareadlocalssd,
           round(q.profintdatawritelocalssd * h.fraction)        as profintdatawritelocalssd,
           round(q.profintdatareads3 * h.fraction)               as profintdatareads3,
           round(q.profintdatawrites3 * h.fraction)              as profintdatawrites3,
           round(q.profremoteextread * h.fraction)               as profremoteextread,
           round(q.profremoteextwrite * h.fraction)              as profremoteextwrite,
           round(q.profreswrites3 * h.fraction)                  as profreswrites3,
           round(q.proffsmeta * h.fraction)                      as proffsmeta,
           round(q.profdataexchangenet * h.fraction)             as profdataexchangenet,
           round(q.profdataexchangemsg * h.fraction)             as profdataexchangemsg,
           round(q.profcontrolplanemsg * h.fraction)             as profcontrolplanemsg,
           round(q.profos * h.fraction)                          as profos,
           round(q.profmutex * h.fraction)                       as profmutex,
           round(q.profsetup * h.fraction)                       as profsetup,
           round(q.profsetupmesh * h.fraction)                   as profsetupmesh,
           round(q.profteardown * h.fraction)                    as profteardown,
           round(q.profscanrso * h.fraction)                     as profscanrso,
           round(q.profxtscanrso * h.fraction)                   as profxtscanrso,
           round(q.profprojrso * h.fraction)                     as profprojrso,
           round(q.profsortrso * h.fraction)                     as profsortrso,
           round(q.proffilterrso * h.fraction)                   as proffilterrso,
           round(q.profresrso * h.fraction)                      as profresrso,
           round(q.profdmlrso * h.fraction)                      as profdmlrso,
           round(q.profhjrso * h.fraction)                       as profhjrso,
           round(q.profbufrso * h.fraction)                      as profbufrso,
           round(q.profflatrso * h.fraction)                     as profflatrso,
           round(q.profbloomrso * h.fraction)                    as profbloomrso,
           round(q.profaggrso * h.fraction)                      as profaggrso,
           round(q.profbandrso * h.fraction)                     as profbandrso,
           round(q.profpercentilerso * h.fraction)               as profpercentilerso,
           round(q.profudtfrso * h.fraction)                     as profudtfrso,
           q.memoryused,
           h.time                                                as timeslice,
           h.fraction                                            as timefraction
    from queries q,
         HourlyTimeExplosion h
    where q.queryid = h.queryid);
commit;