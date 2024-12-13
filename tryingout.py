
#schema6
def data_processing_user(self,num_of_users):
    user_data = self.generate_users(num_of_users)
    user_data_df = self.spark.createDataFrame(user_data)
    return user_data_df


def enrichment_output(
    fact_manufacturing_defects_df,
    Dim_Assembly_Line_df,
    Dim_Device_Type_df,
    Dim_Sensor_df,
    Dim_Defect_Type_df
):

    # Perform joins to enrich the fact table with dimension data
    enriched_output = (
        fact_manufacturing_defects_df
        .join(Dim_Assembly_Line_df, on="assembly_line_id", how="left")
        .join(Dim_Device_Type_df, on="device_type_id", how="left")
        .join(Dim_Sensor_df, on="sensor_id", how="left")
        .join(Dim_Defect_Type_df, on="defect_type_id", how="left")
        .select(
            fact_manufacturing_defects_df[""]
        )


    #)
    return enriched_output

def enrichment_output_df(fact_manufacturing_defects_df, dim_device_type_df, dim_sensor_df, dim_defect_type_df, dim_production_batch_df)
    enriched_output_df = fact_manufacturing_defects_df \
    .join(dim_defect_type_df, "assembly_line_id") \
    .join(dim_device_type_df, "device_type_id") \
    .join(dim_sensor_df, "sensor_id") \
    .join(dim_defect_type_df, "defect_type_id") \
    .join(dim_production_batch_df, "production_batch_id") \


# Join orders_fact_df with products_df to enrich with product details
enriched_df = orders_fact_df \
    .join(products_df, "product_id", "left") \
    .join(vendors_df, "vendor_id", "left") \
    .join(marketplaces_df, "market_id", "left") \
    .join(
        calendar_df,
        (orders_fact_df["year"] == calendar_df["year"]) &
        (orders_fact_df["month"] == calendar_df["month"]) &
        (orders_fact_df["day"] == calendar_df["day"]),
        "left"
    ) \
    .select(
        orders_fact_df["order_id"],                # Order details
        orders_fact_df["product_id"],
        products_df["name"].alias("product_name"), # Product details
        products_df["category"],
        orders_fact_df["vendor_id"],
        vendors_df["name"].alias("vendor_name"),   # Vendor details
        vendors_df["region"],
        orders_fact_df["market_id"],
        marketplaces_df["name"].alias("market_name"), # Marketplace details
        marketplaces_df["type"].alias("market_type"),
        orders_fact_df["order_date"],
        orders_fact_df["order_quantity"],
        orders_fact_df["order_amount"],
        calendar_df["date"].alias("calendar_date"),   # Calendar details
        calendar_df["weekday"],
        calendar_df["is_weekend"]
    )


#schema2
def generate_dim_device_type(self, num_records):
    categories = ["Consumer PC", "Alienware", "Servers", "Desktop", "Gamer"]
    manufacturers = [self.faker.company() for _ in range(num_records)]
    device_data = []  # List to store device entries
    used_ids = set()  # Set to ensure unique IDs

    for _ in range(num_records):
        # Generate unique device_type_id
        device_type_id = self.faker.random_int(min=1, max=1000)
        while device_type_id in used_ids:
            device_type_id = self.faker.random_int(min=1, max=1000)
        used_ids.add(device_type_id)

        # Create a device entry
        device_entry = {
            "device_type_id": device_type_id,  # Unique ID as PRIMARY KEY
            "device_name": self.faker.word().capitalize() + " Device",
            "category": random.choice(categories),
            "manufacturer": random.choice(manufacturers),
            "device_lifespan": self.faker.random_int(min=1, max=20)
        }
        device_data.append(device_entry)

    return device_data

#	1.	Fixed device_type_id: Changed the placeholder string "device_type_id" to dynamically generate a unique ID using faker and ensured uniqueness with a used_ids set.
#	2.	Corrected categories and manufacturers: Changed the strings "categories" and "manufacturers" to reference actual lists.
#	3.	Validated unique ID generation: Prevented duplicate IDs by checking against the used_ids set.
#   4.	Ensured correct random choices: Used random.choice correctly on lists.