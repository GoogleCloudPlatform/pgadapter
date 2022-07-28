select r.threads, r.batch_size, r.operation_count,
       max(n.throughput) as native_throughput, max(j17.throughput) as java17_throughput, max(j8.throughput) as java8_throughput,
       case
         when max(n.throughput) > max(j17.throughput) and max(n.throughput) > max(j8.throughput) then 'native'
         when max(j17.throughput) > max(n.throughput) and max(j17.throughput) > max(j8.throughput) then 'java17'
         when max(j8.throughput) > max(n.throughput) and max(j8.throughput) > max(j17.throughput) then 'java8'
         else 'unknown'
       end as highest_throughput,
       min(n.read_avg) as native_read_avg, min(j17.read_avg) as java17, min(j8.read_avg) as java8,
       case
         when min(n.read_avg) < min(j17.read_avg) and min(n.read_avg) < min(j8.read_avg) then 'native'
         when min(j17.read_avg) < min(n.read_avg) and min(j17.read_avg) < min(j8.read_avg) then 'java17'
         when min(j8.read_avg) < min(n.read_avg) and min(j8.read_avg) < min(j17.read_avg) then 'java8'
         else 'unknown'
       end as lowest_read_avg,
       min(n.read_p95) as native_read_p95, min(j17.read_p95) as java17, min(j8.read_p95) as java8,
       case
         when min(n.read_p95) < min(j17.read_p95) and min(n.read_p95) < min(j8.read_p95) then 'native'
         when min(j17.read_p95) < min(n.read_p95) and min(j17.read_p95) < min(j8.read_p95) then 'java17'
         when min(j8.read_p95) < min(n.read_p95) and min(j8.read_p95) < min(j17.read_p95) then 'java8'
         else 'unknown'
       end as lowest_read_p95
from run r
inner join run n using (threads, batch_size, operation_count)
inner join run j8 using (threads, batch_size, operation_count)
inner join run j17 using (threads, batch_size, operation_count)
where n.deployment = 'native-uds'
  and  j8.deployment = 'jar-java8-uds'
  and j17.deployment = 'jar-java17-uds'
group by r.threads, r.batch_size, r.operation_count
order by r.threads, r.batch_size, r.operation_count
;
