mod mongodb;
use mongodb::CommandService::command_service_client::CommandServiceClient;

use bson::{Bson, Document, Uuid};
use bytes::{Buf, BufMut};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::marker::PhantomData;
use tonic::codec::{Codec, DecodeBuf, Decoder, EncodeBuf, Encoder};
use tonic::{Request, Response, Status};

#[derive(Serialize, Deserialize)]
struct MongotCursorBatch {
    ok: u8,
    errmsg: Option<String>,
    cursor: Option<MongotCursorResult>,
    explain: Option<Bson>,
    vars: Option<Bson>,
}

#[derive(Serialize, Deserialize)]
struct MongotCursorResult {
    id: u32,
    #[serde(rename = "nextBatch")]
    next_batch: Vec<VectorSearchResult>,
    ns: String,
    r#type: Option<ResultType>,
}

#[derive(Serialize, Deserialize)]
struct VectorSearchResult {
    #[serde(rename = "_id")]
    id: Bson,
}

#[derive(Serialize, Deserialize)]
enum ResultType {
    Results,
    Meta,
}

#[derive(Serialize, Deserialize)]
struct VectorSearchCommand {
    #[serde(rename = "vectorSearch")]
    pub vector_search: String,
    #[serde(rename = "$db")]
    pub db: String,
    #[serde(rename = "collectionUUID")]
    pub collection_uuid: Uuid,
    pub path: String,
    #[serde(rename = "queryVector")]
    pub query_vector: Vec<i64>,
    pub index: String,
    pub limit: i64,
    #[serde(rename = "numCandidates")]
    pub num_candidates: i64,
}

#[derive(Debug)]
pub struct BsonEncoder<T>(PhantomData<T>);

impl<T: serde::Serialize> Encoder for BsonEncoder<T> {
    type Item = T;
    type Error = Status;

    fn encode(&mut self, item: Self::Item, buf: &mut EncodeBuf<'_>) -> Result<(), Self::Error> {
        let doc: Document =
            bson::to_document(&item).map_err(|e| Status::internal(e.to_string()))?;
        let bytes: Vec<u8> = bson::to_vec(&doc).map_err(|e| Status::internal(e.to_string()))?;
        // write bson bytes to the buffer
        buf.writer()
            .write_all(&bytes)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct BsonDecoder<U>(PhantomData<U>);

impl<U: serde::de::DeserializeOwned> Decoder for BsonDecoder<U> {
    type Item = U;
    type Error = Status;

    fn decode(&mut self, buf: &mut DecodeBuf<'_>) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        let item: Self::Item =
            bson::from_reader(buf.reader()).map_err(|e| Status::internal(e.to_string()))?;

        Ok(Some(item))
    }
}

#[derive(Debug, Clone)]
pub struct BsonCodec<T, U>(PhantomData<(T, U)>);

impl<T, U> Default for BsonCodec<T, U> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

impl<T, U> Codec for BsonCodec<T, U>
where
    T: serde::Serialize + Send + 'static,
    U: serde::de::DeserializeOwned + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = BsonEncoder<T>;
    type Decoder = BsonDecoder<U>;

    fn encoder(&mut self) -> Self::Encoder {
        BsonEncoder(PhantomData)
    }

    fn decoder(&mut self) -> Self::Decoder {
        BsonDecoder(PhantomData)
    }
}

// generates grpc stubs
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let vector_service = tonic_build::manual::Service::builder()
        .package("mongodb")
        .name("CommandService")
        .method(
            tonic_build::manual::Method::builder()
                .name("vectorSearch")
                .route_name("vectorSearch")
                .input_type("VectorSearchCommand")
                .output_type("MongotCursorBatch")
                .codec_path("BsonCodec")
                .build(),
        )
        .build();

    tonic_build::manual::Builder::new()
        .out_dir("src/mongodb")
        .compile(&[vector_service]);

    Ok(())
}

#[tokio::test]
async fn test_query() {
    let mut client = CommandServiceClient::connect("http://localhost:27029")
        .await
        .unwrap();

    let request: Request<VectorSearchCommand> = Request::new(VectorSearchCommand {
        vector_search: String::from("test"),
        db: String::from("test"),
        collection_uuid: Uuid::parse_str("e954c0a6-61b3-477d-859c-2bac22e865a2").unwrap(),
        index: String::from("default"),
        path: String::from("description"),
        query_vector: vec![1, 2],
        num_candidates: 50,
        limit: 5,
    });

    let response: Response<MongotCursorBatch> = client.vectorSearch(request).await.unwrap();

    println!("RESPONSE={:?}", response.get_ref().ok);
}
