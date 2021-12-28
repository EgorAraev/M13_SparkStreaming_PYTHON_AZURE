from ast import literal_eval
from pyspark.sql import SparkSession, Row
from pyspark.streaming import StreamingContext
import pyspark.sql.functions as f

STAY_TYPES = ['err', 'short', 'standard', 'extended', 'long']


def get_stay_types(stay_dur_col):
    """Maps stay duration to stay types"""
    return f.when((stay_dur_col <= 0) | (stay_dur_col > 30) | (stay_dur_col.isNull()), 'err')\
        .when(stay_dur_col == 1, 'short')\
        .when((stay_dur_col >= 2) & (stay_dur_col <= 7), 'standard')\
        .when((stay_dur_col > 7) & (stay_dur_col <= 14), 'extended')\
        .when((stay_dur_col > 14) & (stay_dur_col <= 30), 'long')


def get_stay_type_columns(row_as_dict):
    """Get a dict containing only stay type key-value pairs"""
    return {key: value for key, value in row_as_dict.items() if key in STAY_TYPES}


def get_most_pop(row_as_dict):
    """Get most popular stay type"""
    return {'most_popular_type': max(row_as_dict, key=row_as_dict.get)}


def mapping_func(row, kids=False):
    """Maps state to hotel_id"""
    row_as_dict = row.asDict()
    state = get_stay_type_columns(row_as_dict)
    state.update(get_most_pop(state))
    # Additional logic applied to 2017 data
    if kids:
        state['with_kids'] = row_as_dict['num_kids'] > 0
    return row_as_dict['hotel_id'], state


def _state_agg_func(a, b):
    """Aggregates updates with previous state"""
    for key in STAY_TYPES:
        a[key] = a.get(key, 0) + b.get(key, 0)
    # Recalculate most popular stay type
    a.update(get_most_pop(get_stay_type_columns(a)))
    # Add info about kids
    a['with_kids'] = a.get('with_kids', False) or b.get('with_kids', False)
    return a


def update_func(new_batch):
    """Updates state"""
    global state
    new_batch = new_batch.rdd.map(lambda x: mapping_func(x, kids=True))
    state = state.union(new_batch).aggregateByKey({}, _state_agg_func, _state_agg_func)


def main_logic(df, kids=False):
    """Counts kids and check-ins by type for each hotel"""
    res = df.withColumn('stay_dur', f.datediff('srch_co', 'srch_ci'))\
        .withColumn('stay_type', get_stay_types(f.col('stay_dur')))\
        .drop('srch_co', 'srch_ci')\
        .groupBy('hotel_id').pivot('stay_type').agg(f.count('stay_type')).na.fill(0)
    # Additional logic for 2017 data
    if kids:
        kids_count = df.groupBy('hotel_id').agg(f.sum('srch_children_cnt').alias('num_kids'))
        res = res.join(kids_count, 'hotel_id')
    return res.drop('srch_children_cnt')


if __name__ == '__main__':
    session = SparkSession.builder.enableHiveSupport().getOrCreate()
    session.sparkContext.setLogLevel('ERROR')
    # Read 2016 data
    data16 = session.sql("SELECT * "
                         "FROM orc.`hdfs:///201_HW_Dataset/valid_joined_data/ci_year=2016` "
                         "WHERE (avg_tmpr_c > 0)")
    # Get schema for later use
    schema = data16.schema
    # Select only relevant fields
    data16 = data16.select('hotel_id', 'srch_co', 'srch_ci', 'srch_children_cnt')
    state = f.broadcast(main_logic(data16)).cache()
    state = state.rdd.map(mapping_func)
    # Read 2017 data in streaming manner
    data17 = session.readStream.schema(schema).orc('hdfs:///201_HW_Dataset/valid_joined_data/ci_year=2017')
    # Select only relevant fields
    data17 = data17.select('hotel_id', 'srch_co', 'srch_ci', 'srch_children_cnt')
    # For each processed mini-batch, update the state
    query = data17.writeStream.outputMode('append').foreachBatch(
        lambda batch, _: update_func(main_logic(batch, kids=True))).start()
    query.processAllAvailable()
    # Save final state
    state.saveAsTextFile('hdfs:///201_HW_Dataset/final_state/')




