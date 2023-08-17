# Packages
# remotes::install_github("DataWookie/feedeR")
library(feedeR)
library(bigrquery)
library(DBI)

# Bigquery auth
bigrquery::bq_auth(path = "g1feed/g1feed-135d76612977.json", use_oob = TRUE)

# Connection
con <- dbConnect(
  bigrquery::bigquery(),
  project = "g1feed",
  dataset = "g1_brasil",
  billing = "g1feed"
)

# Fetch feed
new_feed <- feedeR::feed.extract(url = "https://g1.globo.com/dynamo/rss2.xml")
new_feed <- new_feed$items

# Retrive last feed
last_feed <- readRDS(file = "g1feed/last_feed.rds")

# Select new feed
updated_feed <- subset(new_feed, !(hash %in% last_feed$hash))

# Save current feed as last one
saveRDS(object = new_feed, file = "last_feed.rds")

# Write new results
if(nrow(updated_feed) > 0){
  if(DBI::dbExistsTable(con, "g1_brasil")){
    DBI::dbWriteTable(conn = con, name = "g1_brasil", value = updated_feed, append = TRUE)
  } else {
    DBI::dbWriteTable(conn = con, name = "g1_brasil", value = updated_feed, append = FALSE)
  }
}


