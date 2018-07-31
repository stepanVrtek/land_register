setwd('~/Desktop/devmons/land_register/')

# Get all KU files
files1 = list.files('.', pattern = 'KU*')
files2 = list.files('test1_20KU_2LV_00-23-43/', pattern = 'KU*')
files2 = paste0('test1_20KU_2LV_00-23-43/', files2)

files = c(files1, files2)

db = data.frame()

# rbind all the shit
for (file in files) {
  try({
    csv_raw = read.csv(file, header = FALSE) 
    db = rbind(db, csv_raw)
  })
}

# rename columns
colnames(db) = c('KU_code', 'LV_code', 'download_latency', 'response_code', 'valid_request', 'request_type')
db[,'code'] = paste0(db[,'KU_code'], db[,'LV_code'])

avg_download_latency = mean(db[,'download_latency'])
median_download_latency = median(db[,'download_latency'])
quantile(db[,'download_latency'], c(.25, .75, .85, .9, .95)) 
avg_download_latency
median_download_latency

# density of download_latency
d <- density(db[,'download_latency'])
plot(d, main="Kernel Density of download time per request", xlab = 'Seconds')
polygon(d, col="#004561")

# concurrent requests
concurrent = c(4, 8, 12, 24, 32)
requests_per_second_free = concurrent/avg_download_latency
requests_per_second_small = 1.8*concurrent/avg_download_latency # 6 hours a day with 32 concurrent requests and speed 4 times faster than free
requests_per_second_large = 3.5*concurrent/avg_download_latency # 18 hours a day with 32 concurrent requests and speed 4 times faster than free

# requests per day
requests_per_second_free*60*60*24
requests_per_second_small*60*60*24
requests_per_second_large*60*60*24

# requests per 14 days
requests_per_second_free*60*60*24*14
requests_per_second_small*60*60*24*14
requests_per_second_large*60*60*24*14

# average no. of requests per land unit, those with no. request less than or equal 3 are non-existing land units
existing_lvs = table(db[,'code'])[which(table(db[,'code']) > 3)]
nonexisting_lvs = table(db[,'code'])[which(table(db[,'code']) <= 3)]
avg_existing_lvs = mean(existing_lvs)
avg_nonexisting_lvs = mean(nonexisting_lvs)
existing_ratio = length(existing_lvs)/(length(existing_lvs) + length(nonexisting_lvs))
avg_existing_lvs
avg_nonexisting_lvs
existing_ratio

# valid requests
valid_requests = dim(db[which(db[,'valid_request'] == 'True'),])[1]
invalid_requests = dim(db[which(db[,'valid_request'] == 'False'),])[1]
valid_requests
invalid_requests

# struktura csv je KU_code, LV_code, download_latency, response_code, valid_request(true alebo false), request_type (stranka, na ktoru posielame request)


