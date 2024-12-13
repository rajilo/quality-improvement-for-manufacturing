
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, to_date, current_date
import random
import uuid

from faker import Faker
from datetime import datetime, timedelta



class UpstreamDataProvider:

# Define a constructor - this will be used for all schemas

    def __init__(self):
        self.faker = Faker()
        self.spark = SparkSession.builder\
        .appName("quality_improvement_for_manufacturing")\
        .config("spark.executor.memory", "10g")\
        .config("spark.driver.memory", "2g")\
        .config("spark.sql.shuffle.partitions", "50") \
        .getOrCreate()


# Data generation

#schema 1

    def generate_fact_manufacturing_defects(self, num_records):
        defects_data = []

        for _ in range(num_records):
            defect_entry = {
                "fact_id": self.faker.uuid4(),
                "assembly_line_id": self.faker.random_int(min=1000, max=9999),
                "device_type_id": self.faker.random_int(min=1, max=100),
                "sensor_id": self.faker.random_int(min=1, max=500),
                "defect_type_id": self.faker.random_int(min=1, max=20),
                "defect_count": self.faker.random_int(min=1, max=50),  # Realistic defect count
                "defect_timestamp": self.faker.date_time_this_year().isoformat(),
                "resolution_time_minutes": self.faker.random_int(min=1, max=480),  # Minutes for resolution (up to 8 hours)
                "production_batch_id": self.faker.random_int(min=10000000, max=99999999)  # Ensures 8-digit number
            }
            defects_data.append(defect_entry)

        return defects_data

if __name__ == '__main__':

    test = UpstreamDataProvider()
    data =  test.generate_fact_manufacturing_defects(1000)
    fact_manufacturing_df= test.spark.createDataFrame(data)
    fact_manufacturing_df.show()
    print(fact_manufacturing_df.count())

    fact_manufacturing_df.printSchema()
    fact_manufacturing_df.write.format("parquet").mode("overwrite").save("upstream_data")
    fact_manufacturing_df.write.format("csv").save("csv")


#schema2

def generate_dim_device_type(self, num_records):
    categories = ["Consumer PC", "Mouse", "Servers", "Desktop", "Gamer"]
    manufacturers = [self.faker.company() for _ in range(num_records)]
    device_data_type = []
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
            "device_lifespan": self.faker.random_int(min=1, max=20)
        }
        device_data_type.append(device_entry)

    return device_data_type

if __name__ == "__main__":
    test = UpstreamDataProvider()
    generator = test.generate_dim_device_type(1000)
    device_data_df = generator.generate_dim_device_type(generator)
    device_data_df.show()
    print(device_data_df.count())



# #schema3
def generate_dim_sensor_data(self, num_records):
    sensor_types = ["Temperature", "Pressure", "Humidity", "Vibration", "Proximity",
                    "Optical", "Flow", "Ultrasonic"]  # Define valid sensor types
    sensor_data = []  # Initialize an empty list for sensor entries

    for _ in range(num_records):
        # Create a sensor entry
        sensor_entry = {
            "sensor_id": self.faker.uuid4(),  # Generate a unique sensor ID
            "sensor_name": self.faker.word().capitalize() + " Sensor",  # Generate a sensor name
            "sensor_type": random.choice(sensor_types),  # Randomly select a sensor type
            "assembly_line_id": self.faker.random_int(min=1000, max=9999)  # Generate assembly line ID
        }
        sensor_data.append(sensor_entry)  # Add the entry to the sensor_data list

    return sensor_data  # Return the list of sensor entries


if __name__ == "__main__":
    test = UpstreamDataProvider()
    generate_sensor = test.generate_dim_sensor_data(1000)
    sensor_data_df = generate_sensor.generate_dim_device_type(generate_sensor)
    sensor_data_df.show()
    print(sensor_data_df.count())



# #schema4

def generate_dim_defect_type(self, num_records):
    severity_levels = ["Low", "Medium", "High", "Critical"]  # Define severity levels
    defect_types = []  # Initialize an empty list for defect types

    for _ in range(num_records):
        # Create a defect type entry
        defect_type = {
            "defect_type_id": self.faker.unique.random_int(min=1, max=1000),  # Generate unique defect type ID
            "defect_name": self.faker.word().capitalize(),  # Generate defect name
            "severity": random.choice(severity_levels),  # Randomly select severity level
            "description": self.faker.sentence(nb_words=10)  # Generate a description
        }
        defect_types.append(defect_type)  # Add the entry to the defect_types list

    return defect_types  # Return the list of defect types

if __name__ == "__main__":
    test = UpstreamDataProvider()
    defect_type_data = test.generate_dim_defect_type(1000)
    defect_type = {
        "defect_type_id": self.faker.unique.random_int(min=1, max=1000),  # Generate unique defect type ID
        "defect_name": self.faker.word().capitalize(),  # Generate defect name
        "severity": random.choice(severity_levels),  # Randomly select severity level
        "description": self.faker.sentence(nb_words=10)  # Generate a description
    }
    defect_type_df = spark.createDataFrame(defect_type_data, defect_type)
    defect_type_df.show(truncate=False)
    defect_type_df = defect_type_data.generate_dim_device_type(defect_type_data)
    defect_type_df.show()
    print(defect_type_df.count())


# #schema5
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
            "batch_status": random.choice(["In Progress", "Completed", "Failed", "Pending"])
        }
        batch_data.append(batch_entry)

    return batch_data
#
#
#
# def enrichment_output_df(fact_manufacturing_defects_df, dim_device_type_df, dim_sensor_df, dim_defect_type_df, dim_production_batch_df)
#     enriched_output_df = fact_manufacturing_defects_df \
#     .join(dim_defect_type_df, "assembly_line_id") \
#     .join(dim_device_type_df, "device_type_id") \
#     .join(dim_sensor_df, "sensor_id") \
#     .join(dim_defect_type_df, "defect_type_id") \
#     .join(dim_production_batch_df, "production_batch_id") \









    # results = data.data_processsing_user(100)
    # results.show()
    # print(results.count())


    #user_data_df.write.format("parquet").mode("overwrite").save("location")

   # spark-submit --packages org.apache.hadoop:haddop-aws-location

