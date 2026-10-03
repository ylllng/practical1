#!/usr/bin/env python3
"""Local Lakehouse lab runner. Python 3.9+, standard library only."""
import argparse, csv, json, math, os, platform, statistics, subprocess, time
from pathlib import Path
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from http.client import HTTPException

ROOT = Path(__file__).resolve().parent
OUT = ROOT / 'results'
COLUMNS = {
 'customer': 'custkey, name, acctbal, nationkey, address, phone, comment',
 'orders': 'orderkey, custkey, orderdate',
 'lineitem': 'orderkey, linenumber, extendedprice, discount, returnflag',
 'nation': 'nationkey, name',
}
COMBINATIONS = [('PARQUET','GZIP'),('PARQUET','ZSTD'),('ORC','ZSTD'),('ORC','SNAPPY')]

def http(url, body=None, headers=None):
    data = None if body is None else (body.encode() if isinstance(body,str) else json.dumps(body).encode())
    h = {'Content-Type':'application/json', **(headers or {})}
    with urlopen(Request(url, data=data, headers=h), timeout=60) as r:
        content=r.read()
        return json.loads(content) if content else {}

def wait(url, seconds=300):
    deadline=time.monotonic()+seconds
    while time.monotonic()<deadline:
        try: return http(url)
        except HTTPError as e:
            if e.code not in (429, 500, 502, 503, 504): raise
            time.sleep(2)
        except (URLError, OSError, HTTPException): time.sleep(2)
    raise RuntimeError('Сервис не запустился: '+url)

def sql(query):
    started=time.perf_counter()
    page=http('http://localhost:8080/v1/statement', query,
              {'Content-Type':'text/plain', 'X-Trino-User':'student'})
    rows=[]; columns=[]; deadline=time.monotonic()+3600
    while True:
        if 'error' in page: raise RuntimeError(json.dumps(page['error'],ensure_ascii=False))
        columns=page.get('columns', columns); rows.extend(page.get('data', []))
        if not page.get('nextUri'): break
        if time.monotonic()>deadline: raise RuntimeError('Запрос превысил 60 минут: '+query)
        uri=page['nextUri'].replace('http://trino:8080','http://localhost:8080')
        page=http(uri, headers={'X-Trino-User':'student'})
    entry={'sql':query,'query_id':page.get('id'),'wall_seconds':time.perf_counter()-started,
           'stats':page.get('stats',{}),'rows_returned':len(rows)}
    with (OUT/'queries.jsonl').open('a',encoding='utf-8') as f: f.write(json.dumps(entry,ensure_ascii=False)+'\n')
    return rows, [c['name'] for c in columns], entry['wall_seconds']

def scalar(query): return sql(query)[0][0][0]

def csv_save(name, cols, rows):
    with (OUT/name).open('w',encoding='utf-8-sig',newline='') as f:
        w=csv.writer(f); w.writerow(cols); w.writerows(rows)

def equals(actual, expected):
    if len(actual)!=len(expected): return False
    for a,b in zip(actual,expected):
        if a[0]!=b[0]: return False
        for i,(x,y) in enumerate(zip(a,b)):
            if i==2:
                if not math.isclose(float(x),float(y),rel_tol=1e-12,abs_tol=1e-6): return False
            elif x!=y: return False
    return True

def bootstrap():
    wait('http://localhost:8181/health')
    info = wait('http://localhost:8181/management/v1/info')
    if not info.get('bootstrapped', False):
        try: http('http://localhost:8181/management/v1/bootstrap',{'accept-terms-of-use':True})
        except HTTPError as e:
            if e.code != 409: raise
    project=http('http://localhost:8181/management/v1/project')['project-id']
    warehouses=http('http://localhost:8181/management/v1/warehouse', headers={'x-project-id':project})
    items=warehouses.get('warehouses',[])
    if not any(w.get('name', w.get('warehouse-name'))=='warehouse' for w in items):
        http('http://localhost:8181/management/v1/warehouse',{
          'warehouse-name':'warehouse','project-id':project,
          'storage-profile':{'type':'s3','bucket':'warehouse','endpoint':'http://minio:9000',
             'region':'us-east-1','path-style-access':True,'flavor':'minio','sts-enabled':False},
          'storage-credential':{'type':'s3','credential-type':'access-key',
             'aws-access-key-id':'minioadmin','aws-secret-access-key':'minioadmin123'}})
    wait('http://localhost:8080/v1/info')
    # SQL readiness is stronger than an HTTP readiness check.
    scalar('SELECT 1')

def script(name):
    for q in (ROOT/'sql'/name).read_text().split(';'):
        if q.strip(): sql(q.strip())

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--sf',choices=['0.1','1','10'],default='1')
    ap.add_argument('--fio',default='[Укажите ФИО как в VK Education]')
    ap.add_argument('--repeats',type=int,default=5)
    args=ap.parse_args()
    if args.repeats<3: ap.error('--repeats должен быть не меньше 3')
    OUT.mkdir(exist_ok=True)
    for file in ('queries.jsonl','benchmark.csv','layers.csv','checks.json','gold_top20.csv','отчет.md'):
        (OUT/file).unlink(missing_ok=True)
    bootstrap()
    (OUT/'docker-ps.txt').write_text(subprocess.check_output(['docker','compose','ps'],cwd=ROOT,text=True))
    images=subprocess.check_output(['docker','compose','images','--format','json'],cwd=ROOT,text=True)
    (OUT/'images.json').write_text(images)
    (OUT/'environment.json').write_text(json.dumps({'platform':platform.platform(),
       'cpu_count_host':os.cpu_count(),'sf':args.sf,'trino':scalar('SELECT version()'),
       'repeats':args.repeats},ensure_ascii=False,indent=2))
    q10=(ROOT/'sql/q10.sql').read_text()
    print('1/5. Генерация контрольного Q10',flush=True)
    expected,cols,_=sql(q10.format(schema='tpch.sf'+args.sf))
    if len(expected)!=20: raise RuntimeError('Контрольный Q10 не вернул 20 строк')
    csv_save('reference_top20.csv',cols,expected)
    for schema in ('bronze','silver','gold','bench_parquet_gzip','bench_parquet_zstd','bench_orc_zstd','bench_orc_snappy','demo'):
        sql('CREATE SCHEMA IF NOT EXISTS iceberg.'+schema)
    # Reset only the known tables managed by this lab.
    for table in ('customer_nation','returned_order_items'): sql('DROP TABLE IF EXISTS iceberg.silver.'+table)
    sql('DROP TABLE IF EXISTS iceberg.gold.q10_top20')
    bench=[]; checks={}
    for fmt,codec in COMBINATIONS:
        schema='bench_'+fmt.lower()+'_'+codec.lower()
        print('2/5. Bronze / '+fmt+' '+codec,flush=True)
        times=[]
        for table,col in COLUMNS.items():
            sql(f'DROP TABLE IF EXISTS iceberg.{schema}.{table}')
            _,_,t=sql(f"CREATE TABLE iceberg.{schema}.{table} WITH (format='{fmt}', compression_codec='{codec}') AS SELECT {col} FROM tpch.sf{args.sf}.{table}")
            times.append(t)
        for table in COLUMNS: sql(f'ANALYZE iceberg.{schema}.{table}')
        result,_,cold=sql(q10.format(schema='iceberg.'+schema))
        if not equals(result,expected): raise RuntimeError('Несовпадение Q10: '+schema)
        warm=[]
        for _ in range(args.repeats):
            result,_,elapsed=sql(q10.format(schema='iceberg.'+schema)); warm.append(elapsed)
            if not equals(result,expected): raise RuntimeError('Результат изменился: '+schema)
        size=sum(int(scalar(f'SELECT coalesce(sum(file_size_in_bytes),0) FROM iceberg.{schema}."{t}$files"')) for t in COLUMNS)
        bench.append([fmt,codec,round(sum(times),6),round(cold,6),round(statistics.median(warm),6),round(min(warm),6),round(max(warm),6),size,round(size/1e6,6)])
        csv_save('benchmark.csv',['format','codec','write_seconds','first_query_seconds','median_warm_seconds','min_warm_seconds','max_warm_seconds','data_bytes','data_MB'],bench)
        checks[schema+'_matches_q10']=True
    print('3/5. Silver и Gold',flush=True)
    # Reuse measured Parquet/ZSTD Iceberg tables through read-only Bronze views;
    # no second physical copy or misleading duplicate storage accounting.
    for table in COLUMNS: sql(f'CREATE OR REPLACE VIEW iceberg.bronze.{table} AS SELECT * FROM iceberg.bench_parquet_zstd.{table}')
    script('silver.sql'); script('gold.sql')
    gold,cols,_=sql('SELECT * FROM iceberg.gold.q10_top20 ORDER BY revenue DESC, custkey')
    if not equals(gold,expected): raise RuntimeError('Gold отличается от Q10')
    checks['gold_matches_q10']=True
    checks['customer_rows_preserved']=scalar('SELECT count(*) FROM iceberg.silver.customer_nation')==scalar('SELECT count(*) FROM iceberg.bronze.customer')
    checks['invalid_lineitems']=scalar('SELECT count(*) FROM iceberg.bronze.lineitem WHERE extendedprice IS NULL OR extendedprice<0 OR discount IS NULL OR discount NOT BETWEEN 0 AND 1')
    checks['duplicate_lineitem_keys']=scalar('SELECT count(*) FROM (SELECT orderkey,linenumber FROM iceberg.bronze.lineitem GROUP BY 1,2 HAVING count(*)>1)')
    checks['orphan_orders']=scalar('SELECT count(*) FROM iceberg.bronze.orders o LEFT JOIN iceberg.bronze.customer c ON o.custkey=c.custkey WHERE c.custkey IS NULL')
    checks['orphan_lineitems']=scalar('SELECT count(*) FROM iceberg.bronze.lineitem l LEFT JOIN iceberg.bronze.orders o ON l.orderkey=o.orderkey WHERE o.orderkey IS NULL')
    if not checks['customer_rows_preserved'] or any(checks[k]!=0 for k in ('invalid_lineitems','duplicate_lineitem_keys','orphan_orders','orphan_lineitems')):
        raise RuntimeError('Проверки качества не пройдены: '+str(checks))
    csv_save('gold_top20.csv',cols,gold)
    layers=[]
    mappings=[('Bronze',t,'bench_parquet_zstd',t) for t in COLUMNS]
    mappings += [('Silver',t,'silver',t) for t in ('customer_nation','returned_order_items')]+[('Gold','q10_top20','gold','q10_top20')]
    for layer,label,sch,t in mappings:
        count=scalar(f'SELECT count(*) FROM iceberg.{sch}.{t}')
        size=scalar(f'SELECT coalesce(sum(file_size_in_bytes),0) FROM iceberg.{sch}."{t}$files"')
        layers.append([layer,label,count,round(int(size)/1e6,6),'PARQUET','ZSTD','нет'])
    csv_save('layers.csv',['layer','table','rows','data_MB','format','codec','partitioning'],layers)
    print('4/5. Schema evolution и time travel',flush=True)
    sql('DROP TABLE IF EXISTS iceberg.demo.customer_nation')
    sql("CREATE TABLE iceberg.demo.customer_nation WITH (format='PARQUET',compression_codec='ZSTD') AS SELECT * FROM iceberg.silver.customer_nation")
    before=scalar('SELECT count(*) FROM iceberg.demo.customer_nation')
    sql('ALTER TABLE iceberg.demo.customer_nation ADD COLUMN source_note VARCHAR')
    sql('ALTER TABLE iceberg.demo.customer_nation RENAME COLUMN source_note TO data_source')
    checks['schema_evolution_nulls']=scalar('SELECT count(*) FROM iceberg.demo.customer_nation WHERE data_source IS NULL')==before
    snapshot=scalar('SELECT snapshot_id FROM iceberg.demo."customer_nation$snapshots" ORDER BY committed_at DESC LIMIT 1')
    sql('INSERT INTO iceberg.demo.customer_nation SELECT * FROM iceberg.demo.customer_nation ORDER BY custkey LIMIT 1')
    checks['demo_insert']=scalar('SELECT count(*) FROM iceberg.demo.customer_nation')==before+1
    historical=scalar(f'SELECT count(*) FROM iceberg.demo.customer_nation FOR VERSION AS OF {snapshot}')
    checks['time_travel']=historical==before
    sql(f'ALTER TABLE iceberg.demo.customer_nation EXECUTE rollback_to_snapshot({snapshot})')
    checks['rollback']=scalar('SELECT count(*) FROM iceberg.demo.customer_nation')==before
    if not all(checks[k] for k in ('schema_evolution_nulls','demo_insert','time_travel','rollback')): raise RuntimeError('Демонстрация Iceberg не пройдена')
    checks['snapshot_id']=snapshot
    (OUT/'checks.json').write_text(json.dumps(checks,ensure_ascii=False,indent=2))
    (OUT/'time_travel.sql').write_text(f'SELECT * FROM iceberg.demo.customer_nation FOR VERSION AS OF {snapshot};\nALTER TABLE iceberg.demo.customer_nation EXECUTE rollback_to_snapshot({snapshot});\n')
    report=(ROOT/'report-template.md').read_text()
    report=report.replace('[Укажите ФИО как в VK Education]',args.fio).replace('{{SF}}',args.sf)
    report=report.replace('{{STATUS}}','Основной ELT, сравнение форматов, schema evolution и time travel выполнены локальным скриптом. Результат Gold сопоставлен с исходным Q10; проверки пройдены. Подключение Superset и скриншоты оформляются по инструкции в README.md.')
    for layer in ('Bronze','Silver','Gold'):
        table='\n'.join('| '+' | '.join(map(str,row[1:]))+' |' for row in layers if row[0]==layer)
        report=report.replace('{{'+layer.upper()+'_METRICS}}',table)
    report=report.replace('{{BENCHMARK}}','\n'.join('| '+' | '.join(map(str,row))+' |' for row in bench))
    fastest=min(bench,key=lambda r:r[4]); smallest=min(bench,key=lambda r:r[7])
    report=report.replace('{{BENCH_CONCLUSION}}',f'На этой машине наименьшая медиана тёплого Q10 получена для {fastest[0]}/{fastest[1]}: {fastest[4]} с; наименьший размер данных — {smallest[0]}/{smallest[1]}: {smallest[8]} МБ. Это локальные измерения, а не универсальный рейтинг кодеков.')
    report=report.replace('{{SNAPSHOT}}',str(snapshot))
    (OUT/'отчет.md').write_text(report,encoding='utf-8')
    print('5/5. Готово: results/отчет.md и results/gold_top20.csv. Теперь подключите Superset по README.',flush=True)

if __name__=='__main__':
    try: main()
    except Exception as e:
        print('ОШИБКА:',e,flush=True)
        raise SystemExit(1)
