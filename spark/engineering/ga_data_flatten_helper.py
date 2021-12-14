from pyspark.sql import functions as func
from pyspark.sql.types import *

class GoogleAnalyticsDataFlattenHelper:
    """Google analytics data flatten helper that is used to flatten the data export from Google bigquery to HDFS JSON data

    Args:
        spark(object): This arg is to input the spark object created by SparkSession.
        cdate(str): This arg is to store the data running date.

    Attributes:
        spark(object): This attribute is for storing the spark object.
        cdate(str): This attribute is for storing the input date.
        DF_LIST_NAME(list(str)): This attribute is for saving the expected flatten dataframe.
        RAW_BASEPATH(str): This attribute is for saving the raw json data path in HDFS.
        BASEPATH(str): This attribute is for saving the expected flatten data.
    """
    DF_LIST_NAME = ['general', 
                    'customDimensions', 
                    'hits', 
                    'hits_customDimensions', 
                    'hits_customMetrics', 
                    'hits_product',
                    'hits_product_customDimensions',
                    'hits_product_customMetrics',
                    'hits_promotion']
    RAW_BASEPATH = """RAW DATA PATH EXPORT FROM BQ TO HDFS; JSON FORMAT DATA EXPECTED"""
    BASEPATH = """FLATTEN DATA SAVE PATH"""
    
    def __init__(self, spark, cdate, *args, **kwargs):
        self.spark = spark
        self.cdate = cdate
        
    def run(self, save=True, *args, **kwargs):
        """Main function to run the whole pipeline

        Args:
            save(bool): Boolean to decide to save or return data.

        Returns:
            None if save = False, otherwise list of spark dataframe and correspoding dataframe name.
        """
        # ------------------------------
        # 1. Read the raw ga and generate the unique session id
        # ------------------------------
        path = self.RAW_BASEPATH + '/cdate={}'.format(self.cdate)
        
        df = self.spark.read.json(path)
        df = df.withColumn('_id', func.concat_ws('_', func.col('fullVisitorId'), func.col('visitId'), func.lit(self.cdate)))
        # ------------------------------
        # 2. Flatten the dataframe by calling the flatten_ga function
        # ------------------------------
        df_list = self.flatten_ga(df)
        # ------------------------------
        # 3. Save or return the data
        # ------------------------------
        if len(self.DF_LIST_NAME) == len(df_list):
            if save:
                for col_name, gen_df in zip(self.DF_LIST_NAME, df_list):
                    if gen_df != None:
                        print('saving dataframe: {}'.format(col_name), end='...')
                        save_path = self.BASEPATH + '/{}/cdate={}'.format(col_name, self.cdate)   
                        gen_df.repartition(1).write.save(save_path, mode='append', format='parquet')
                        print('Done!')
                return None
            else:
                return self.DF_LIST_NAME, df_list
        else:
            raise ValueError("Different length between df_list and df_list_name. Please check if there is any new dataframe.")
        
    def flatten_ga(self, df, base_col=None, field=None, prefix=""):
        """Flattening the GA data with different spark data types

        Args:
            df(pyspark.sql.DataFrame): DataFrame that is flattened.
            base_col(str): Selected columns that want to carry to the sub level and save.
            field(str): Checking of which dataframe that need to carry more base_col.
            prefix(str): Prefix of the columns of the dataframe.

        Returns:
            output_list: list of dataframe that is flattened.
        """
        # ------------------------------
        # 1. Define the variable
        # ------------------------------
        schema = df.schema
        norm_field_cols, array_field_cols = list(), list()
        sec_keys = {'hits': ['hitNumber']}
        output_list = list()
        # ------------------------------
        # 2. Seperate the dataType: if the data type is struct / string / boolean, 
        #    group it into one group (norm_field_cols), also save the array type for 
        #    further processing (array_field_cols)
        # ------------------------------
        for field_col in schema:
            if isinstance(field_col.dataType, (StructType, StringType, BooleanType)):
                norm_field_cols.append(field_col)
            elif isinstance(field_col.dataType, ArrayType):
                array_field_cols.append(field_col)
            else:
                raise ValueError("Unexpected data type: {}".format(field_col))
        # ------------------------------
        # 3. Flatten dataframe: 
        #    3.1 Simply flatten and select all the columns in norm_field_cols
        #    3.2 For the columns in array_field_cols, explode the columns first, 
        #        then call the flatten_ga recursively to ensure all the columns
        #        is in norm_field_cols
        # ------------------------------
        if base_col == None:
            base_col = ['_id']
        if (field != None) & (field in sec_keys.keys()):
            base_col += [field + "_" + c for c in sec_keys[field]]
        # Normal field dataframe
        norm_field_df = df.select(self.flatten_struct(norm_field_cols))
        output_list.append(norm_field_df)
        # Array field dataframe
        for field_col in array_field_cols:
            field_name = field_col.name
            if df.filter(func.size(func.col(field_name)) > 0).count() > 0:
                trans = df.select(*base_col, field_name)
                trans = trans.withColumn(field_name, func.explode(field_name))
                trans = trans.select(*base_col,
                                     *[func.col(field_name + '.' + c.name).alias(field_name + '_' + c.name) for c in trans.select(field_name).schema[0].dataType])
                output_list += self.flatten_ga(trans, field=field_name, base_col=base_col)
            else:
                output_list.append(None)

        return output_list
        
    def flatten_struct(self, schema, prefix=""):
        """Flattening the column base on the column data type

        Args:
            schema(pyspark.sql.DataFrame.schema): DataFrame schema
            prefix(str): Prefix of the columns of the dataframe that need to be added.

        Returns:
            result: List of column name that is selected.
        """
        result = list()
        for field in schema:
            if isinstance(field.dataType, StructType):
                result += self.flatten_struct(field.dataType, prefix=prefix + field.name + '.')
            else:
                result.append(func.col(prefix + field.name).alias((prefix + field.name).replace('.', '_')))

        return result