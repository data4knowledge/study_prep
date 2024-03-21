// MATCH (s:Subject)<-[:FOR_SUBJECT_REL]-(d:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)
// MATCH (dc)-[:INSTANCES_REL]-(sai1:ScheduledActivityInstance)
// MATCH (sai1)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
// MATCH (dc)-[:DC_TO_MAIN_TIMELINE]-(sai2:ScheduledActivityInstance)
// MATCH (sai2)-[:ENCOUNTER_REL]-(e:Encounter)
// RETURN s,d,dc,sai1,sai2,t,e
// LIMIT 25

MATCH (s:Subject)<-[:FOR_SUBJECT_REL]-(d:DataPoint)-[:FOR_DC_REL]->(dc:DataContract)
MATCH (dc)-[:INSTANCES_REL]-(sai1:ScheduledActivityInstance)
MATCH (sai1)<-[:RELATIVE_FROM_SCHEDULED_INSTANCE_REL]-(t:Timing)
MATCH (dc)-[:INSTANCES_REL]->(sai2:ScheduledActivityInstance)
MATCH (sai2)-[:ENCOUNTER_REL]-(e:Encounter)
MATCH (dc)-[:PROPERTIES_REL]->(bcp:BiomedicalConceptProperty)
MATCH (bcp)<-[:PROPERTIES_REL]-(bc:BiomedicalConcept)
RETURN s,d,dc,sai1,sai2,t,e,bcp,bc
LIMIT 5