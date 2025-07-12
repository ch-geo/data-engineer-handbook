from chispa.dataframe_comparer import assert_df_equality
from ..jobs.scd_actor_generation import do_actors_scd_transformation
from collections import namedtuple

Actor = namedtuple("Actor", "actor actorid quality_class current_year is_active")
ActorCummulative = namedtuple("ActorCummulative", "actor actorid quality_class is_active start_date end_date")


def test_scd_generation(spark):
    source_data = [
        Actor("Michael Jordan", 1, "star", 1999, 1),
        Actor("Michael Jordan", 1, "star", 2000, 1),
        Actor("Michael Jordan", 1, "bad", 2001, 1),
    ]
    source_df = spark.createDataFrame(source_data)

    actual_df = do_actors_scd_transformation(spark, source_df)
    expected_data = [
        ActorCummulative("Michael Jordan", 1, "star", 1, 1999, 2000),
        ActorCummulative("Michael Jordan", 1, "bad", 1, 2001, 2001),
    ]
    expected_df = spark.createDataFrame(expected_data)
    assert_df_equality(actual_df, expected_df)
