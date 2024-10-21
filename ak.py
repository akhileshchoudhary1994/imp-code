import os
import sys
from datetime import datetime, timedelta, timezone

from airflow import DAG

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from PipelineConf import default_args, mysql_cmcollect_password
from PipelineFactory import pyspark_factory, get_job_args, get_schedule_interval
from functools import partial

with DAG(
        'Creditmate-MCA-Scoring',
        default_args=default_args,
        description='Creditmate ML Paytm MCA Scoring DAG',
        schedule_interval=get_schedule_interval('15 5 * * *'),
        start_date = datetime(2021,1,1),
        start_date = datetime(2021,1,1),
        catchup=False,
        default_view='graph',
        params = {"end_date":""},
        params = {"end_date":""},
        orientation='TB',
        tags=['creditmate', 'scoring', 'MCA']
) as dag:
    end_date = "{{ dag_run.conf.get('end_date', dag_run.start_date.strftime('%Y-%m-%d')) if dag_run.conf and dag_run.conf.get('end_date') else dag_run.start_date.strftime('%Y-%m-%d')}}"
    env = "{{ dag_run.conf.get('env', var.value.env) if dag_run.conf else var.value.env }}"
    # env = "{{ dag_run.conf.get('env', var.value.env) if dag_run.conf and dag_run.conf.get('end_date') else var.value.env }}"
    lender = "paytm_mca"

    
    # Curry the function for repetitive arguments.
    curr_job_args = partial(get_job_args, lender=lender, env=env, end_date=end_date)  # type: ignore
    # Conf Setting for a python process only reduce the instances to 1 and low memory and increase the driver
    python_proces_conf = {
        "spark.driver.memory": '20g',
        "spark.executor.instances": '1',
        "spark.executor.memory": '1g',
        "spark.memory.fraction": "0.1"
    }
    # config for jobs that needs more resources
    big_job_conf = {
        "spark.executor.instances": str(100),
        'spark.executor.memory': '12g',
        'spark.executor.cores': '2',
    }
    small_job_conf = {
        "spark.executor.instances": str(50),
        'spark.executor.memory': '8g',
        'spark.executor.cores': '2',
        'spark.executor.heartbeatInterval': '3000s',
        'spark.network.timeout': "3600s",
    }
    


# Index job
    refined_combinations = pyspark_factory(dag,job_name="Refined_Combs", job_args=get_job_args(job="strategy.scoring_pipelines.refine_combinations",env=env,
                                                                          end_date=end_date,lender=lender),
                            job_conf=big_job_conf)
    # Index job
    index = pyspark_factory(dag,job_name="Indexes", job_args=get_job_args(job="strategy.scoring_pipelines.index_job",env=env,
                                                                          end_date=end_date,lender=lender),
                            job_conf=small_job_conf)
    
    # lp feats
    lp_feats = pyspark_factory(dag,job_name="lp_feats", job_args=get_job_args(job="strategy.scoring_pipelines.lp_feats",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # cm feats
    cm_feats = pyspark_factory(dag,job_name="cm_feats", job_args=get_job_args(job="strategy.scoring_pipelines.Cm_feats",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=big_job_conf)
    
    # ct feats
    ct_feats = pyspark_factory(dag,job_name="ct_feats", job_args=get_job_args(job="strategy.scoring_pipelines.Ct_feats",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=big_job_conf)
    
    # disposition feats
    disposition_feats = pyspark_factory(dag,job_name="disposition_feats", job_args=get_job_args(job="strategy.scoring_pipelines.disposition_feats",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # merchant attributes feats
    merchant_attributes_feats = pyspark_factory(dag,job_name="merchant_attributes_feats", job_args=get_job_args(job="strategy.scoring_pipelines.merchant_attributes",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # repayment feats
    repayment_feats = pyspark_factory(dag,job_name="repayment_feats", job_args=get_job_args(job="strategy.scoring_pipelines.repayment_feats",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=big_job_conf)
    
    # consolidation feats
    consolidation_feats = pyspark_factory(dag,job_name="consolidation_feats", job_args=get_job_args(job="strategy.scoring_pipelines.consolidate_feats",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # preprocessing feats
    preprocessing_feats = pyspark_factory(dag,job_name="preprocessing_feats", job_args=get_job_args(job="strategy.scoring_pipelines.preprocessing",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # gmv feats
    #gmv_feats = pyspark_factory(dag,job_name="gmv_feats", job_args=get_job_args(job="strategy.scoring_pipelines.gmv_feats",env=env,
    #                                                                      end_date=end_date,lender=lender),
    #                    job_conf=small_job_conf)
    
    #val_comb processing feats
    val_comb_process = pyspark_factory(dag,job_name="val_comb_processing_feats", job_args=get_job_args(job="strategy.scoring_pipelines.valid_comb_processing",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=big_job_conf)
    
    # generate state embeddings feats
    generate_state_embeddings = pyspark_factory(dag,job_name="generate_state_embeddings", job_args=get_job_args(job="strategy.scoring_pipelines.generate_state_embeddings",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # score action model
    score_action_model = pyspark_factory(dag,job_name="score_action_model", job_args=get_job_args(job="strategy.scoring_pipelines.sb_action_scoring",env=env,
                                                                          end_date=end_date,lender=lender),
                        job_conf=small_job_conf)
    
    # Create manual strategy
    # manual_strategy_module = pyspark_factory(dag,job_name="manual_strategy", job_args=get_job_args(job="strategy.scoring_pipelines.manual_strategy",env=env,
    #                                                                       end_date=end_date,lender=lender),
    #                     job_conf=small_job_conf)
    

    # Define dependency chart
    index.set_upstream(refined_combinations)
    lp_feats.set_upstream(index)
    cm_feats.set_upstream(lp_feats)
    ct_feats.set_upstream(cm_feats)
    disposition_feats.set_upstream(ct_feats)
    merchant_attributes_feats.set_upstream(disposition_feats)
    repayment_feats.set_upstream(merchant_attributes_feats)
    consolidation_feats.set_upstream(repayment_feats)
    preprocessing_feats.set_upstream(consolidation_feats)
    #gmv_feats.set_upstream(preprocessing_feats)
    #generate_state_embeddings.set_upstream(gmv_feats)

    generate_state_embeddings.set_upstream(preprocessing_feats)
    val_comb_process.set_upstream(generate_state_embeddings)
    score_action_model.set_upstream([generate_state_embeddings,val_comb_process])
    #manual_strategy_module.set_upstream([gmv_feats,score_action_model])

     



 