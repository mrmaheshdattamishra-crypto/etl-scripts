# Similar ticket reference: SCRUM-7 - Customer by Region (existing implementation found)
# Extending existing customer_regional_categorization table or creating enhanced version

Feature: customer_regional_categorization
  As a data analyst
  I want to extend customer tables with US region categorization
  So that I can analyze customer patterns by geographic regions

  Background:
    Given the following source tables:
      | table_name      | columns |
      | customer        | c_customer_sk:NUMBER, c_customer_id:STRING, c_current_addr_sk:NUMBER, c_first_name:STRING, c_last_name:STRING, c_preferred_cust_flag:STRING, c_birth_day:NUMBER, c_birth_month:NUMBER, c_birth_year:NUMBER, c_birth_country:STRING, c_login:STRING, c_email_address:STRING |
      | customer_address | ca_address_sk:NUMBER, ca_address_id:STRING, ca_street_number:STRING, ca_street_name:STRING, ca_city:STRING, ca_county:STRING, ca_state:STRING, ca_zip:STRING, ca_country:STRING, ca_location_type:STRING |

    And the following target table:
      | table_name                      | columns |
      | customer_regional_categorization | customer_key:NUMBER, customer_id:STRING, customer_name:STRING, address_key:NUMBER, city:STRING, county:STRING, state:STRING, country:STRING, region:STRING, location_type:STRING |

  Scenario: Create customer regional categorization data model
    Given I have customer data in the customer table
    And I have address data in the customer_address table
    When I join customer table with customer_address table on c_current_addr_sk equals ca_address_sk
    Then I should map customer_key from c_customer_sk
    And I should map customer_id from c_customer_id
    And I should concatenate c_first_name and c_last_name with space separator to create customer_name
    And I should map address_key from ca_address_sk
    And I should map city from ca_city
    And I should map county from ca_county  
    And I should map state from ca_state
    And I should map country from ca_country
    And I should map location_type from ca_location_type
    And I should categorize region based on state mapping where:
      | states | region |
      | CT, ME, MA, NH, NJ, NY, PA, RI, VT | Northeast |
      | IL, IN, IA, KS, MI, MN, MO, NE, ND, OH, SD, WI | Midwest |
      | DE, FL, GA, MD, NC, SC, VA, DC, WV, AL, KY, MS, TN, AR, LA, OK, TX | South |
      | AZ, CO, ID, MT, NV, UT, WY, AK, CA, HI, OR, WA | West |
    And I should filter for country equals 'United States' to focus on US customers only