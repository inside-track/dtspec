# Data Transform Spec

Realistic - A more realistic example of a test spec that highlights most of the features of dtspec.
Includes multiple, sources, targets, scenarios, factories, and slightly more
complex transformations.

In this example, we have 4 source tables (raw_students, raw_schools,
raw_classes, and dim_date), which get combined and aggregated into
two target tables (student_classes and students_per_school).

Data sources:

* raw_students - This is the main source for student data
* raw_schools - 
* raw_classes - 
* dim_date - 

Data targets:

* student_classes - Denormalized table with one record per student per class
* students_per_school - 

## Factories common to all scenarios

### Factory: SomeStudents
A few example students.  Yes, I am mixing geek universes.  So what?

**raw_students**:

| id   | external_id | school_id | name   |
| -    | -           | -         | -      |
| stu1 | stu1        | sch1      | Buffy  |
| stu2 | stu2        | sch1      | Willow |
| stu3 | stu3        | sch2      | Bill   |
| stu4 | stu4        | sch2      | Ted    |

**raw_schools**:

| id   | name      |
| -    | -         |
| sch1 | Sunnydale |
| sch2 | San Dimas |

### Factory: StudentsWithClasses


**raw_classes**:

| student_id | name            | start_date |
| -          | -               | -          |
| stu1       | Applied Stabby  | 2001-09-08 |
| stu2       | Good Spells     | 2002-01-09 |
| stu3       | Station         | 2002-09-07 |
| stu4       | Being Excellent | 2002-09-07 |

### Factory: DateDimension


**dim_date**:

| date       | season      |
| -          | -           |
| 2001-09-08 | Fall 2001   |
| 2002-01-09 | Spring 2002 |
| 2002-06-01 | Summer 2002 |
| 2002-09-07 | Fall 2002   |

# Scenario: DenormalizingStudentClasses

Description: Shows how we take all our raw data and denormalize it
and how we handle some common edge cases.

### Factory common to all cases in this scenario

Parents:

* StudentsWithClasses
* DateDimension


## Case: BasicDenormalization

Description: This is what happens when everything works normally
* Expected target data

  **student_classes**:
  
  | card_id | name   | school_name | class_name      | season      |
  | -       | -      | -           | -               | -           |
  | stu1    | Buffy  | Sunnydale   | Applied Stabby  | Fall 2001   |
  | stu2    | Willow | Sunnydale   | Good Spells     | Spring 2002 |
  | stu3    | Bill   | San Dimas   | Station         | Fall 2002   |
  | stu4    | Ted    | San Dimas   | Being Excellent | Fall 2002   |
  
## Case: MissingClasses

Description: Students without classes are excluded from denormalized table
* Given the source data

  **raw_classes**:
  
  | student_id | name            |
  | -          | -               |
  | stu1       | Applied Stabby  |
  | stu2       | Good Spells     |
  
* Expected target data

  **student_classes**:
  
  | card_id | name   | school_name | class_name      |
  | -       | -      | -           | -               |
  | stu1    | Buffy  | Sunnydale   | Applied Stabby  |
  | stu2    | Willow | Sunnydale   | Good Spells     |
  
## Case: MultipleClasses

Description: Students with multiple classes have multiple records
* Given the source data

  **raw_classes**:
  
  | student_id | name            |
  | -          | -               |
  | stu1       | Applied Stabby  |
  | stu2       | Good Spells     |
  | stu2       | Season 6 Spells |
  | stu3       | Station         |
  | stu4       | Being Excellent |
  | stu4       | Station         |
  
* Expected target data

  **student_classes**:
  
  | card_id | name   | school_name | class_name      |
  | -       | -      | -           | -               |
  | stu1    | Buffy  | Sunnydale   | Applied Stabby  |
  | stu2    | Willow | Sunnydale   | Good Spells     |
  | stu2    | Willow | Sunnydale   | Season 6 Spells |
  | stu3    | Bill   | San Dimas   | Station         |
  | stu4    | Ted    | San Dimas   | Being Excellent |
  | stu4    | Ted    | San Dimas   | Station         |
  
# Scenario: StudentAggregation

Description: Counts students per school
### Factory common to all cases in this scenario

Parents:

* SomeStudents


## Case: StudentAggregation

Description: 
* Expected target data

  **students_per_school**:
  
  | school_name | number_of_students |
  | -           | -                  |
  | Sunnydale   | 2                  |
  | San Dimas   | 2                  |
  