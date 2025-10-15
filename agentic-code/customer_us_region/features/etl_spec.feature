# Referenced similar ticket: SCRUM-7 - Customer by Region
# Leveraging existing customer table structure and address relationship

Feature: customer_us_region
  As a business analyst
  I want to categorize customers by US regions
  So that I can analyze customer distribution and behavior by geographic regions

  Background:
    Given the following source tables exist:
      | table_name       | columns |
      | customer         | c_customer_sk:NUMBER, c_customer_id:STRING, c_current_addr_sk:NUMBER, c_first_name:STRING, c_last_name:STRING, c_preferred_cust_flag:STRING, c_birth_day:NUMBER, c_birth_month:NUMBER, c_birth_year:NUMBER, c_birth_country:STRING, c_email_address:STRING |
      | customer_address | ca_address_sk:NUMBER, ca_address_id:STRING, ca_street_number:STRING, ca_street_name:STRING, ca_city:STRING, ca_county:STRING, ca_state:STRING, ca_zip:STRING, ca_country:STRING, ca_location_type:STRING |

    And the target table should be:
      | table_name           | columns |
      | customer_us_region   | c_customer_sk:NUMBER, c_customer_id:STRING, c_first_name:STRING, c_last_name:STRING, c_preferred_cust_flag:STRING, c_birth_day:NUMBER, c_birth_month:NUMBER, c_birth_year:NUMBER, c_birth_country:STRING, c_email_address:STRING, ca_state:STRING, ca_city:STRING, ca_county:STRING, ca_zip:STRING, us_region:STRING, us_region_code:STRING |

  Scenario: Create customer table with US region categorization
    Given I need to join customer with customer_address tables
    When I join customer table with customer_address table on c_current_addr_sk equals ca_address_sk
    And I map us_region based on ca_state using the following logic:
      | us_region | states |
      | Northeast | ME, NH, VT, MA, RI, CT, NY, NJ, PA |
      | Southeast | DE, MD, DC, VA, WV, KY, TN, NC, SC, GA, FL, AL, MS, AR, LA |
      | Midwest   | OH, IN, IL, MI, WI, MN, IA, MO, ND, SD, NE, KS |
      | Southwest | TX, OK, NM, AZ |
      | West      | MT, WY, CO, UT, ID, WA, OR, NV, CA, AK, HI |
    And I map us_region_code as first two letters of us_region in uppercase
    And I filter records where ca_country equals United States or USA
    Then I should create customer_us_region table with all customer information plus region categorization
    And the mapping should preserve all original customer fields
    And the mapping should add ca_state, ca_city, ca_county, ca_zip from address table
    And the mapping should add calculated us_region and us_region_code fields
    And records with null or invalid states should have us_region as Unknown and us_region_code as UK