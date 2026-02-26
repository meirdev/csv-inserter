use bytes::Bytes;
use clickhouse::Client;
use log::info;

#[derive(Debug, thiserror::Error)]
pub enum InserterError {
    #[error("ClickHouse error: {0}")]
    ClickHouse(#[from] clickhouse::error::Error),
}

pub struct ClickHouseInserter {
    client: Client,
    table: String,
    fields: Option<Vec<String>>,
    has_header: bool,
}

impl ClickHouseInserter {
    pub fn new(
        url: &str,
        database: &str,
        user: &str,
        password: &str,
        table: String,
        fields: Option<Vec<String>>,
        has_header: bool,
        async_insert: bool,
    ) -> Self {
        let mut client = Client::default()
            .with_url(url)
            .with_database(database)
            .with_user(user)
            .with_password(password);

        if async_insert {
            client = client
                .with_option("async_insert", "1")
                .with_option("wait_for_async_insert", "1");
        }

        Self {
            client,
            table,
            fields,
            has_header,
        }
    }

    pub async fn insert(&self, content: Vec<u8>) -> Result<(), InserterError> {
        info!("Inserting into table '{}'", self.table);

        let format = if self.has_header {
            "CSVWithNames"
        } else {
            "CSV"
        };

        let query = if let Some(fields) = &self.fields {
            let columns = fields.join(", ");
            format!("INSERT INTO {} ({}) FORMAT {}", self.table, columns, format)
        } else {
            format!("INSERT INTO {} FORMAT {}", self.table, format)
        };

        let mut insert = self.client.insert_formatted_with(&query);
        insert.send(Bytes::from(content)).await?;
        insert.end().await?;

        info!("Insert completed");
        Ok(())
    }
}
