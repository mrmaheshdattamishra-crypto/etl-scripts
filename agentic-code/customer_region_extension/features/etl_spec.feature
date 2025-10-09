# Similar ticket reference: SCRUM-7 - Customer by Region (exact match for US region categorization)

Feature: customer_region_extension
  As a data analyst
  I want to extend customer tables with US region categorization
  So that I can analyze customers by geographical regions

  Background:
    Given source schema:
      | table_name        | column_name        | data_type |
      | customer          | c_customer_sk      | NUMBER    |
      | customer          | c_customer_id      | STRING    |
      | customer          | c_current_addr_sk  | NUMBER    |
      | customer          | c_first_name       | STRING    |
      | customer          | c_last_name        | STRING    |
      | customer_address  | ca_address_sk      | NUMBER    |
      | customer_address  | ca_state           | STRING    |
      | customer_address  | ca_city            | STRING    |
      | customer_address  | ca_county          | STRING    |

    And target schema:
      | table_name           | column_name        | data_type |
      | customer_with_region | c_customer_sk      | NUMBER    |
      | customer_with_region | c_customer_id      | STRING    |
      | customer_with_region | c_first_name       | STRING    |
      | customer_with_region | c_last_name        | STRING    |
      | customer_with_region | state              | STRING    |
      | customer_with_region | city               | STRING    |
      | customer_with_region | county             | STRING    |
      | customer_with_region | us_region          | STRING    |
      | customer_with_region | region_code        | STRING    |

  Scenario: Create customer table with US region categorization
    Given I have customer data with address information
    When I join customer table with customer_address table on current address key
    Then I should create customer_with_region table

    And the join relationship should be:
      customer.c_current_addr_sk = customer_address.ca_address_sk

    And the mapping should be:
      c_customer_sk maps to c_customer_sk as is
      c_customer_id maps to c_customer_id as is  
      c_first_name maps to c_first_name as is
      c_last_name maps to c_last_name as is
      ca_state maps to state as is
      ca_city maps to city as is
      ca_county maps to county as is
      
      us_region maps based on state using following logic:
        when state in AL AR DE FL GA KY LA MD MS NC SC TN VA WV then Northeast
        when state in IL IN IA KS MI MN MO NE ND OH SD WI then Midwest  
        when state in AZ CO ID MT NV NM UT WY then Mountain
        when state in AK CA HI OR WA then Pacific
        when state in CT ME MA NH NJ NY PA RI VT then Northeast
        when state in TX OK then South
        else Unknown
        
      region_code maps based on us_region using following logic:
        when us_region is Northeast then NE
        when us_region is Midwest then MW
        when us_region is Mountain then MT
        when us_region is Pacific then PC
        when us_region is South then SO
        else UN