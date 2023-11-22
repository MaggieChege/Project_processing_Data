import csv
import logging
import psycopg2
import argparse
from dataclasses import asdict, dataclass, field
from typing import List,Optional
from datetime import datetime
from uuid import UUID, uuid4
import psycopg2


@dataclass
class User:
    uuid: UUID = field(default_factory=uuid4)
    phoneNumber: str = None
    createdAt: datetime = field(default_factory=lambda: datetime.now())
    updatedAt: datetime = field(default_factory=lambda: datetime.now())
    nTransactions: int = 0

    def validate(self):
        if self.phoneNumber and not self.validate_phone_number(self.phoneNumber):
            raise ValueError("Invalid phone number format")

        if self.nTransactions < 0:
            raise ValueError("nTransactions cannot be negative")

    @staticmethod
    def validate_phone_number(phone_number: str) -> bool:
        return phone_number.isdigit()

@dataclass
class Transaction:
    date: int
    externalId: str
    agentPhoneNumber: str
    transactionType: str
    amount: float
    balance: float
    receiverPhoneNumber:str
    commission: float
    category: Optional[str] = None
    uuid: Optional[str] = None
    status: Optional[str] = None
    userUuid: Optional[str] = None
    updateTimestamp: Optional[str] = None
    source: Optional[str] = None
    
class CSVToDB:
    def __init__(self, conn, table_name, data_class):
        self.conn = conn
        self.table_name = table_name
        self.data_class = data_class
        self.cursor = self.conn.cursor()
        self.ensure_tables_exist() 
        self.user_cache = self.get_user_cache()
        
        
    def ensure_tables_exist(self):
        with self.conn.cursor() as cursor:
            # Check and create user table
            cursor.execute("""
                CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
                CREATE TABLE IF NOT EXISTS public."user" (
                    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
                    "phoneNumber" character varying,
                    "createdAt" timestamp without time zone NOT NULL DEFAULT now(),
                    "updatedAt" timestamp without time zone NOT NULL DEFAULT now(),
                    "nTransactions" bigint,
                    CONSTRAINT "PK_a95e949168be7b7ece1a2382fed" PRIMARY KEY (uuid),
                    CONSTRAINT "UQ_f2578043e491921209f5dadd080" UNIQUE ("phoneNumber")
                );
            """)
            self.conn.commit()

            # Check and create transaction table
            cursor.execute("""
                -- DROP TABLE IF EXISTS public."transaction";

                CREATE TABLE IF NOT EXISTS public."transaction" (
                    uuid uuid NOT NULL DEFAULT uuid_generate_v4(),
                    mobile character varying,
                    status character varying,
                    category character varying,
                    "userUuid" uuid NOT NULL,
                    balance numeric(20,2),
                    commission numeric(20,2),
                    amount numeric(20,2),
                    "requestTimestamp" bigint NOT NULL,
                    "updateTimestamp" bigint NOT NULL,
                    source character varying,
                    "externalId" character varying,
                    CONSTRAINT "PK_fcce0ce5cc7762e90d2cc7e2307" PRIMARY KEY (uuid),
                    CONSTRAINT "FK_00197c2fde23b7c0f6b69d0b6a2" FOREIGN KEY ("userUuid")
                        REFERENCES public."user" (uuid) MATCH SIMPLE
                        ON UPDATE NO ACTION
                        ON DELETE NO ACTION
                );
            """)
            self.conn.commit()
            cursor.execute("""


                CREATE OR REPLACE FUNCTION update_transactions_count() RETURNS TRIGGER AS $$
                BEGIN
                    UPDATE public."user"
                    SET "nTransactions" = COALESCE("nTransactions", 0) + 1
                    WHERE uuid = NEW."userUuid";
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                DROP TRIGGER IF EXISTS update_transactions_count_trigger ON transaction;
                CREATE TRIGGER update_transactions_count_trigger
                AFTER INSERT ON transaction
                FOR EACH ROW
                EXECUTE PROCEDURE update_transactions_count();

                         
                         """)
            self.conn.commit()
        logging.info("TABLES HAVE BEEN CREATED")
        
    def get_user_cache(self):
        self.cursor.execute('''SELECT "phoneNumber", uuid FROM "user"''')
        return {phoneNumber: uuid for phoneNumber, uuid in self.cursor.fetchall()}

    def process_csv(self, data):
        try:
            # with open(file_path, newline='') as csvfile:
            reader = csv.DictReader(data)
            self.ensure_tables_exist()
            
            batch = []
            for row in reader:
                #Create an instance of the data class for each row
                transaction = self.data_class(**row)
                
                user_uuid = self.user_cache.get(transaction.agentPhoneNumber)
                if user_uuid is None:
                    self.insert_user(transaction)
                    user_uuid = self.get_user_uuid(transaction.agentPhoneNumber)
                    self.user_cache[transaction.agentPhoneNumber] = user_uuid
                transaction.userUuid = user_uuid
                batch.append(asdict(transaction))
                
                if len(batch) >= 1000:
                    self.insert_batch(batch)
                    batch = []
            if batch:
                self.insert_batch(batch)
                
            self.get_transaction_count() 

        except Exception as e:
            logging.error("Failed to process Data")
            self.conn.rollback()
        else:
            self.conn.commit()

    def insert_user(self, transaction):
    
        self.cursor.execute("""
            INSERT INTO "user" ("phoneNumber", "createdAt", "updatedAt", "nTransactions")
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ("phoneNumber")
            DO NOTHING;
        """, (transaction.agentPhoneNumber, datetime.now(), datetime.now(), 0))
        self.conn.commit()

    def get_user_uuid(self, phone_number):
        self.cursor.execute('''SELECT uuid FROM "user" WHERE "phoneNumber" = %s''', (phone_number,))
        result = self.cursor.fetchone()
        return result[0] if result else None
    
    
    def get_transaction_count(self):
        self.cursor.execute('''SELECT count(*) FROM "transaction" ''')
        count = self.cursor.fetchone()[0]
        print(f'The Transaction count is: {count}')
        self.cursor.close()
        



    def insert_batch(self, batch):
    # Filter out transactions without a 'userUuid'
        valid_transactions = batch
        print(valid_transactions)
        for transaction in batch:
       
            if not valid_transactions:
            # If all transactions are invalid, return early
                logging.info("No valid transactions to insert.")
                return

        # Now, only valid_transactions will be inserted into the database
        self.cursor.executemany(
            f"""
            INSERT INTO {self.table_name} ( mobile, status, category, "userUuid", balance, commission, amount, "requestTimestamp", "updateTimestamp", source, "externalId")
            VALUES ( %(receiverPhoneNumber)s, %(status)s, %(transactionType)s, %(userUuid)s, %(balance)s, %(commission)s, %(amount)s, EXTRACT(EPOCH FROM TIMESTAMP %(date)s)::bigint * 1000, COALESCE(%(updateTimestamp)s, EXTRACT(EPOCH FROM NOW())::bigint * 1000), %(source)s, %(externalId)s)
            """,
            valid_transactions
        )
        self.conn.commit()
        logging.info("Transaction data successfully saved!")
        

        
        
def main():
    logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':
    main()
