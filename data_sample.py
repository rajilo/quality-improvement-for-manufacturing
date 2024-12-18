from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from datetime import timedelta
import random
from faker import Faker
from sqlalchemy import select


class UpstreamDataProvider:

    def __init__(self):
        self.faker = Faker()
        self.spark = SparkSession.builder \
            .appName("Quality Improvement for Manufacturing") \
            .config("spark.executor.memory", "10g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "50") \
            .getOrCreate()

    def generate_fact_manufacturing_defects(self, num_records):
        defects_data = []

        for _ in range(num_records):
            defect_entry = {
                "fact_id": str(self.faker.uuid4()),
                "assembly_line_id": self.faker.random_int(min=1000, max=9999),
                "device_type_id": self.faker.random_int(min=1, max=100),
                "sensor_id": self.faker.random_int(min=1, max=500),
                "defect_type_id": self.faker.random_int(min=1, max=20),
                "defect_count": self.faker.random_int(min=1, max=50),
                "defect_timestamp": self.faker.date_time_this_year().isoformat(),
                "resolution_time_minutes": self.faker.random_int(min=1, max=480),
                "production_batch_id": self.faker.random_int(min=10000000, max=99999999),
            }
            defects_data.append(defect_entry)
        return self.spark.createDataFrame(defects_data)



    def generate_dim_device_type(self, num_records):
        categories = ["Consumer PC", "Mouse", "Servers", "Desktop", "Gamer"]
        manufacturers = [self.faker.company() for _ in range(num_records)]
        device_data = []
        used_ids = set()

        for _ in range(num_records):
            device_type_id = self.faker.random_int(min=1, max=1000)
            while device_type_id in used_ids:
                device_type_id = self.faker.random_int(min=1, max=1000)
            used_ids.add(device_type_id)

            device_entry = {
                "device_type_id": device_type_id,
                "device_name": self.faker.word().capitalize() + " Device",
                "category": random.choice(categories),
                "manufacturer": random.choice(manufacturers),
                "device_lifespan": self.faker.random_int(min=1, max=20),
            }
            device_data.append(device_entry)
        return self.spark.createDataFrame(device_data)



    def generate_dim_sensor_data(self, num_records):
        sensor_types = ["Temperature", "Pressure", "Humidity", "Vibration", "Proximity", "Optical", "Flow", "Ultrasonic"]
        sensor_data = []

        for _ in range(num_records):
            sensor_entry = {
                "sensor_id": str(self.faker.uuid4()),
                "sensor_name": self.faker.word().capitalize() + " Sensor",
                "sensor_type": random.choice(sensor_types),
                "assembly_line_id": self.faker.random_int(min=1000, max=9999),
            }
            sensor_data.append(sensor_entry)
        return self.spark.createDataFrame(sensor_data)



    def generate_dim_defect_type(self, num_records):
        severity_levels = ["Low", "Medium", "High", "Critical"]
        defect_types = []

        for _ in range(num_records):
            defect_type = {
                "defect_type_id": self.faker.unique.random_int(min=1, max=1000),
                "defect_name": self.faker.word().capitalize(),
                "severity": random.choice(severity_levels),
                "description": self.faker.sentence(nb_words=10),
            }
            defect_types.append(defect_type)
        return self.spark.createDataFrame(defect_types)



    def generate_dim_production_batch(self, num_records):
        batch_data = []

        for _ in range(num_records):
            start_time = self.faker.date_time_this_year()
            end_time = start_time + timedelta(hours=random.randint(1, 48))

            batch_entry = {
                "production_batch_id": self.faker.unique.random_int(min=1, max=1000),
                "batch_name": "Batch_" + str(self.faker.unique.random_int(min=1, max=1000)),
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "product_count": self.faker.random_int(min=100, max=10000),
                "batch_status": random.choice(["In Progress", "Completed", "Failed", "Pending"]),
            }
            batch_data.append(batch_entry)
        return self.spark.createDataFrame(batch_data)


if __name__ == "__main__":
    test = UpstreamDataProvider()

    # Schema 1: fact_manufacturing_defects
    fact_df = test.generate_fact_manufacturing_defects(1000)
    fact_df.show()
    print(f"Fact manufacturing defects count: {fact_df.count()}")

    # Schema 2: dim_device_type
    device_type_df = test.generate_dim_device_type(1000)
    device_type_df.show()
    print(f"Device types count: {device_type_df.count()}")

    # Schema 3: dim_sensor_data
    sensor_data_df = test.generate_dim_sensor_data(1000)
    sensor_data_df.show()
    print(f"Sensor data count: {sensor_data_df.count()}")

    # Schema 4: dim_defect_type
    defect_type_df = test.generate_dim_defect_type(1000)
    defect_type_df.show()
    print(f"Defect types count: {defect_type_df.count()}")

    # Schema 5: dim_production_batch
    production_batch_df = test.generate_dim_production_batch(1000)
    production_batch_df.show()
    print(f"Production batches count: {production_batch_df.count()}")



#Data Enrichment

    def enrichment_output_df( fact_manufacturing_defects_df, dim_device_type_df, dim_sensor_df, dim_defect_type_df, dim_production_batch_df, "s3_bucket+enriched_data/"):

        """
        Enriches the fact_manufacturing_defects_df by joining it with dimension tables.

        Parameters:
            fact_manufacturing_defects_df: DataFrame containing fact data.
            dim_device_type_df: DataFrame containing device type dimension data.
            dim_sensor_df: DataFrame containing sensor dimension data.
            dim_defect_type_df: DataFrame containing defect type dimension data.
            dim_production_batch_df: DataFrame containing production batch dimension data.

        Returns:
            DataFrame: Enriched DataFrame after joining all dimensions with the fact table.
        """
        enriched_output_df = fact_manufacturing_defects_df \
            .join(dim_defect_type_df, "defect_type_id", "inner") \
            .join(dim_device_type_df, "device_type_id", "inner") \
            .join(dim_sensor_df, "sensor_id", "inner") \
            .join(dim_production_batch_df, "production_batch_id", "inner")


        return enriched_output_df

    enrichment_output.write.mode("overwrite").parquet(s3_bucket+"enriched_data/")