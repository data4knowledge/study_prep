// 2024-03-21

// ------------------
// Create data contracts from study-service
// NOTE you need to add this too the ne4j.conf: cypher.lenient_create_relationship = true
// To ignore making a relationship MERGE (dc)-[:INSTANCES_REL]->(act_inst) when act_inst is null
// (this is the case if not in the profile timeline):
call apoc.cypher.run("MATCH(s:Study)-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline) where not (tl)<-[:TIMELINE_REL]-()
MATCH(tl)-[r4:INSTANCES_REL]->(act_inst_main:ScheduledActivityInstance)-[r5:ACTIVITY_REL]->(act:Activity)-[r6:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r7:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
return distinct s, 
sd, 
tl, 
act, 
act_inst_main,
act_inst_main.uuid as act_inst_uuid,
null as act_inst,  
bc,
bc_prop
      UNION
MATCH(s:Study)-[r1:VERSIONS_REL]->(StudyVersion)-[r2:STUDY_DESIGNS_REL]->(sd:StudyDesign)
MATCH(sd)-[r3:SCHEDULE_TIMELINES_REL]->(tl:ScheduleTimeline)<-[r4:TIMELINE_REL]-(act_main)<-[r5:ACTIVITY_REL]-(act_inst_main:ScheduledActivityInstance)
MATCH(tl)-[r6:INSTANCES_REL]->(act_inst:ScheduledActivityInstance)-[r7:ACTIVITY_REL]->(act:Activity)-[r8:BIOMEDICAL_CONCEPT_REL]->(bc:BiomedicalConcept)-[r9:PROPERTIES_REL]->(bc_prop:BiomedicalConceptProperty) 
return distinct s, 
sd, 
tl, 
act, 
act_inst_main,
act_inst_main.uuid+'/'+ act_inst.uuid as act_inst_uuid,
act_inst,  
bc,
bc_prop",{}) YIELD value
WITH value.s as s, 
value.sd as sd, 
value.tl as tl, 
value.act as act, 
value.act_inst_main as act_inst_main, 
value.act_inst as act_inst, 
value.bc as bc, 
value.bc_prop as bc_prop, 
value. act_inst_uuid as  act_inst_uuid
WITH s, sd, tl,act,act_inst_main,act_inst,bc,bc_prop,act_inst_uuid
MERGE (dc:DataContract{uri:'https://study.d4k.dk/' + s.name + '/' + act_inst_uuid + '/' + bc_prop.uuid})
      MERGE (dc)-[:PROPERTIES_REL]->(bc_prop)
      MERGE (dc)-[:INSTANCES_REL]->(act_inst)
      MERGE (dc)-[:INSTANCES_REL]->(act_inst_main)
      SET s.uri = 'https://study.d4k.dk/' + s.name

// ------------------
// Get data contract
MATCH (n:DataContract)
return n
limit 10

// Get data contract for sub-timeline
match (msai:ScheduledActivityInstance)<-[:DC_TO_MAIN_TIMELINE]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
where  enc.label = "Screening 1"
and    t.value = 'PT1M'
and    bcp.name = 'Vital Signs Result'
and    bc.name = 'Diastolic Blood Pressure'
return *
limit 3

match (msai:ScheduledActivityInstance)<-[:DC_TO_MAIN_TIMELINE]-(dc:DataContract)-[:INSTANCES_REL]-(ssai:ScheduledActivityInstance)
match (msai)-[:ENCOUNTER_REL]->(enc:Encounter)
match (ssai)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
match (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
return *
limit 3