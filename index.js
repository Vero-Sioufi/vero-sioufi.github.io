// Define global variables to appease standardjs.com linter
/* global tableau, $, Option */

(function () {
  // Initialize dependencies
  const Airtable = require('airtable')
  const tableauConnector = tableau.makeConnector()

  // Define variables
  const airtableFieldTypesToExclude = ['button', 'multipleAttachments']
  const dataTypesToConsiderMetric = [tableau.dataTypeEnum.int, tableau.dataTypeEnum.float]

  // Helper function to replace characters Tableau does not supprot in IDs (credit https://github.com/tagyoureit/InfluxDB_WDC/blob/gh-pages/InfluxDB_WDC.js#L28)
  function replaceSpecialCharsForTableauID (str) {
    const newStr = str.replace(/ /g, '_')
      .replace(/"/g, '_doublequote_')
      .replace(/,/g, '_comma_')
      .replace(/=/g, '_equal_')
      .replace(/\//g, '_fslash_')
      .replace(/-/g, '_dash_')
      .replace(/\./g, '_dot_')
      .replace(/[^A-Za-z0-9_]/g, '_')
    return newStr
  }

  // Helper function to take an airtable field type and return the apropriate Tableau data type
  function determineTableauColumnType (airtableFieldType) {
    // Tableau data types are listed at https://tableau.github.io/webdataconnector/docs/api_ref.html#webdataconnectorapi.datatypeenum
    // Airtable field types are listed at https://airtable.com/api/meta

    // Look at the Airtable field type and return the appropriate Tableau column type
    switch (airtableFieldType) {
      case 'checkbox':
        return tableau.dataTypeEnum.bool
      case 'createdTime':
        return tableau.dataTypeEnum.datetime
      case 'lastModifiedTime':
        return tableau.dataTypeEnum.datetime
      case 'date':
        return tableau.dataTypeEnum.date
      case 'dateTime':
        return tableau.dataTypeEnum.datetime
      case 'number':
        return tableau.dataTypeEnum.float
      case 'currency':
        return tableau.dataTypeEnum.float
      case 'percent':
        return tableau.dataTypeEnum.float
      case 'count':
        return tableau.dataTypeEnum.int
      case 'rating':
        return tableau.dataTypeEnum.int
      case 'duration':
        return tableau.dataTypeEnum.int
      default: // default to a string
        return tableau.dataTypeEnum.string
    }
  }

  // Helper function to determine what value to return for a given raw value and field metadata object combination
  function airtableFieldValueToTableauColumnValue (airtableRawValue, airtableFieldMeta) {
    if (airtableRawValue === undefined) return airtableRawValue

    switch (airtableFieldMeta.type) {
      case 'singleCollaborator':
        return airtableRawValue.email
      case 'multipleCollaborators':
        return airtableRawValue.map(e => e.email).join(',')
      case 'createdBy':
        return airtableRawValue.email
      case 'checkbox':
        return airtableRawValue === true
      case 'lastModifiedBy':
        return airtableRawValue.email
      default: // default stringifying value (Tableau seems to be OK with this for numeric fields)
        return airtableRawValue.toString()
    }
  }

  // Function called when Tableau is ready to pull the schema
  tableauConnector.getSchema = async function (schemaCallback) {
    try {
      // Load connection data from Tableau connector object built upon form submission
      const connectionData = JSON.parse(tableau.connectionData)
      const { BASE_ID, FIELD_NAME_FOR_AIRTABLE_RECORD_ID } = connectionData

      // Setup structure to store field metadata above and beyond what Tableau column schema allows
      const TABLE_FIELD_METADATA = {} // this will be saved back to connectionData at the end of getSchema and used by getData

      // Call Airtable Metadata API
      const baseMetadata = {
          tables: [
            {
              name: 'Vendor Survey',
              fields: [
                { name: 'LDAP', type: 'Text' },
                { name: 'Name', type: 'Text' },
                { name: 'Work Title (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'Interested in Joining AWU', type: 'Single select' },
                { name: 'Overall Satisfaction With Job', type: 'Rating' },
                { name: 'Best Thing About My Job', type: 'Long text (with rich text formatting enabled)' },
                { name: 'What Most Needs to Change', type: 'Long text (with rich text formatting enabled)' },
                { name: 'Hourly Pay', type: 'Currency' },
                { name: 'Annual Pay', type: 'Currency' },
                { name: 'Weekly Pay', type: 'Currency' },
                { name: 'Employer', type: 'Text' },
                { name: 'How Paid', type: 'Single select' },
                { name: 'Pronouns', type: 'Text' },
                { name: 'Race / Ethnicity', type: 'Multiple select' },
                { name: 'Race Other', type: 'Text' },
                { name: 'Gender', type: 'Multiple select' },
                { name: 'Gender Other', type: 'Text' },
                { name: 'I am already represented by a union', type: 'Single select' },
                { name: 'Name of Union', type: 'Text' },
                { name: 'How do you feel about your pay in general?', type: 'Rating' },
                { name: 'Benefits', type: 'Rating' },
                { name: 'Managment', type: 'Rating' },
                { name: 'What do you wish was better?', type: 'Long text (with rich text formatting enabled)' },
                { name: 'What do you wish was better about managment?', type: 'Long text' },
                { name: 'Hours / Schedule', type: 'Rating' },
                { name: 'What do you wish was better about your hours / schedule', type: 'Long text (with rich text formatting enabled)' },
                { name: 'What do you wish was better about pay?', type: 'Long text' },
                { name: 'Have you experienced harrassment on the job?', type: 'Single select' },
                { name: 'What benefits doy ou get?', type: 'Multiple select' },
                { name: 'Field 32', type: 'Number' },
                { name: 'What do you pay per month for insurance through your employer? (Excluding dental and vision)', type: 'Number' },
                { name: 'Age', type: 'Number' },
                { name: 'Sexual Orientation', type: 'Text' },
                { name: 'Nation of Origin', type: 'Text' },
                { name: 'Are you a veteran?', type: 'Single select' },
                { name: 'Are you disabled?', type: 'Single select' },
                { name: 'Have you experienced any form of caste discrimination', type: 'Single select' },
                { name: 'Field 40', type: 'Number' },
                { name: 'How would you rate your employer\'s accomidaton of your disability?', type: 'Rating' },
                { name: 'Have you received a PIP?', type: 'Single select' },
                { name: 'Discriminaton', type: 'Single select' },
                { name: 'Were you born outside the United States or Canada?', type: 'Single select' },
                { name: 'LDAP cleaned', type: 'Formula' },
                { name: 'Calculation', type: 'Formula' },
                { name: 'All Vendors (1/2022)', type: 'Link to another record' },
                { name: 'Created', type: 'Created time' },
                { name: 'Management Chain (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'Employer (Merged)', type: 'Lookup' },
                { name: 'Registered for Info Session', type: 'Multiple select' },
                { name: 'manager', type: 'Lookup' },
                { name: 'Sent 3/31 Email', type: 'Checkbox' },
                { name: 'Work Title (from All Vendors (1/2022)) 2', type: 'Lookup' },
                { name: 'Location (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'Chapter (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'Work Title (from All Vendors (1/2022)) 3', type: 'Lookup' },
                { name: 'First Name (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'Plan (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'LOC Simple (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'start date', type: 'Lookup' },
                { name: 'type', type: 'Lookup' },
                { name: 'city', type: 'Lookup' },
                { name: 'Manager (from All Vendors (1/2022))', type: 'Lookup' },
                { name: 'Managment Chain', type: 'Lookup' },
                { name: 'AWU?', type: 'Text' },
                { name: 'Investigate Standards?', type: 'Multiple select' },
                { name: 'recordID', type: 'Formula' },
                { name: 'Annual Pay Conversion', type: 'Formula' },
                { name: 'ACA Affordability', type: 'Formula' },
                { name: 'Pay Brackets', type: 'Formula' },
                { name: 'Race / Ethnicity to Graph', type: 'Formula' },
                { name: 'Outlyer', type: 'Checkbox' },
                { name: 'Hourly Pay Converstion', type: 'Formula' },
                { name: 'Buildings', type: 'Link to another record' },
                { name: 'Status', type: 'Single select' },
                { name: 'Job Title', type: 'Text' },
                { name: 'Office', type: 'Text' }
              ]
            },
            {
              name: 'Buildings',
              fields: [
                { name: 'Location', type: 'Text' },
                { name: 'Vendors', type: 'Number' },
                { name: 'Employees', type: 'Number' },
                { name: 'Total Workfoce', type: 'Number' },
                { name: 'LOC Simple', type: 'Formula' },
                { name: 'Chapter From LOC Simple', type: 'Formula' },
                { name: 'Vendor Survey', type: 'Link to another record' },
                { name: 'Employer (Merged) (from Vendor Survey)', type: 'Lookup' },
                { name: 'Building', type: 'Text' },
                { name: 'City', type: 'Single select' },
                { name: 'Count of Surveys', type: 'Count' },
                { name: '% Completed Survey', type: 'Formula' },
                { name: 'State', type: 'Single select' },
                { name: 'Zipcode', type: 'Text' },
                { name: 'Type', type: 'Single select' },
                { name: 'website', type: 'URL' },
                { name: 'address 1', type: 'Text' },
                { name: 'What They Do?', type: 'Single select' },
                { name: 'All Vendors (3/2022)', type: 'Link to another record' },
                { name: 'Members', type: 'Count' },
                { name: 'Cost Center', type: 'Rollup' },
                { name: 'Employers', type: 'Rollup' },
                { name: 'Still To Send', type: 'Count' },
                { name: 'Plan', type: 'Rollup' },
                { name: 'Percent Vendor', type: 'Formula' }
              ]
            },
            {
              name: 'All Vendors (3/2022)',
              fields: [
                { name: 'ID', type: 'Formula' },
                { name: 'Represented By', type: 'Single select' },
                { name: 'Employer', type: 'Single select' },
                { name: 'Work Title', type: 'Text' },
                { name: 'Management Chain', type: 'Long text' },
                { name: 'Sector / Area of Interest', type: 'Single select' },
                { name: 'Start Date', type: 'Date' },
                { name: 'Country', type: 'Single select' },
                { name: 'Name', type: 'Long text' },
                { name: 'Manager', type: 'Single select' },
                { name: 'City', type: 'Single select' },
                { name: 'Cost center', type: 'Single select' },
                { name: 'Building', type: 'Long text' },
                { name: 'Cost Center Code', type: 'Single select' },
                { name: 'Skip Manager', type: 'Single select' },
                { name: 'Type', type: 'Single select' },
                { name: 'Location', type: 'Single select' },
                { name: 'Department', type: 'Text' },
                { name: 'LOC Simple', type: 'Formula' },
                { name: 'Member', type: 'Text' },
                { name: 'Chapter From LOC Simple', type: 'Formula' },
                { name: 'Employer Known', type: 'Formula' },
                { name: 'Chapter', type: 'Single select' },
                { name: 'Vendor Survey', type: 'Link to another record' },
                { name: 'Employer (from Vendor Survey)', type: 'Lookup' },
                { name: 'First Name', type: 'Formula' },
                { name: 'Team', type: 'Single select' },
                { name: 'recordID', type: 'Formula' },
                { name: 'Plan', type: 'Text' },
                { name: 'Managers', type: 'Link to another record' },
                { name: 'Employers', type: 'Link to another record' },
                { name: 'Created (from Vendor Survey)', type: 'Lookup' },
                { name: 'State', type: 'Single select' },
                { name: 'Is Manager?', type: 'Checkbox' },
                { name: 'Attended (from Vendor Survey)', type: 'Lookup' },
                { name: 'Annual Pay Conversion (from Vendor Survey)', type: 'Lookup' },
                { name: 'Buildings', type: 'Link to another record' },
                { name: 'Status', type: 'Lookup' },
                { name: 'State Formula', type: 'Formula' },
                { name: 'Abortion Laws', type: 'Formula' },
                { name: 'State From Location', type: 'Formula' },
                { name: 'Abortion Laws Multi-Selec', type: 'Multiple select' },
                { name: 'Count (Vendor Survey)', type: 'Count' }
              ]
            },
            {
              name: 'Managers',
              fields: [
                { name: 'Login', type: 'Long text' },
                { name: 'Name', type: 'Long text' },
                { name: 'Work Title', type: 'Long text' },
                { name: 'Location', type: 'Single select' },
                { name: 'Management Chain', type: 'Long text' },
                { name: 'Vendor Reports', type: 'Count' },
                { name: 'Count of Surveys', type: 'Count' },
                { name: 'Interest In Organizing', type: 'Count' },
                { name: 'Members', type: 'Count' },
                { name: 'Survey Response', type: 'Formula' },
                { name: 'Interest In Organizing %', type: 'Formula' },
                { name: 'Employer (from All Vendors (3/2022))', type: 'Lookup' },
                { name: 'Plan (from All Vendors (3/2022))', type: 'Lookup' },
                { name: 'Region', type: 'Long text' },
                { name: 'Start Date', type: 'Single select' },
                { name: 'City', type: 'Single select' },
                { name: 'Tenure', type: 'Single select' },
                { name: 'Building', type: 'Single select' },
                { name: 'Manager', type: 'Single select' },
                { name: 'Cost Center Code', type: 'Single select' },
                { name: 'Department', type: 'Single select' },
                { name: 'Skip Manager', type: 'Single select' },
                { name: 'FTE Reports', type: 'Number' },
                { name: 'Total Reports', type: 'Number' },
                { name: 'Cost Center', type: 'Single select' },
                { name: 'Type', type: 'Single select' },
                { name: 'Country', type: 'Single select' },
                { name: 'ï»¿Is Manager?', type: 'Number' },
                { name: 'Direct Reports', type: 'Number' },
                { name: 'All Vendors (3/2022)', type: 'Link to another record' },
                { name: 'Employers', type: 'Link to another record' },
                { name: 'Blank Employers', type: 'Count' },
                { name: 'Employer (from All Vendors (3/2022)) 2', type: 'Lookup' },
                { name: 'LOC Simple (from All Vendors (3/2022))', type: 'Lookup' }
              ]
            },
            {
              name: 'Employers',
              fields: [
                { name: 'Employer', type: 'Text' },
                { name: 'Website', type: 'URL' },
                { name: 'Count of Known Vendors', type: 'Count' },
                { name: 'Survey Completed', type: 'Count' },
                { name: 'Interest In Joining AWU', type: 'Count' },
                { name: 'Count of Existing Members', type: 'Count' },
                { name: 'Unions With Some Relationship', type: 'Single select' },
                { name: 'Vendors', type: 'Link to another record' },
                { name: 'Managers', type: 'Link to another record' },
                { name: 'Count of Managers', type: 'Count' },
                { name: 'ID (from Vendors)', type: 'Lookup' },
                { name: 'Members', type: 'Lookup' }
              ]
            }
          ]
        }

      // For each table, create a schema object
      const tableSchemas = baseMetadata.tables.map((tableMeta) => {
        TABLE_FIELD_METADATA[tableMeta.name] = {}

        // For each table field
        const fieldsForTableau = tableMeta.fields.map((fieldMeta) => {
          // Check to see if the field type is in our exclude list
          if (!airtableFieldTypesToExclude.includes(fieldMeta.type)) {
            // Store Airtable field metadata for use later in getData
            TABLE_FIELD_METADATA[tableMeta.name][fieldMeta.name] = fieldMeta
            const dataType = determineTableauColumnType(fieldMeta.type)
            return {
              id: replaceSpecialCharsForTableauID(fieldMeta.name),
              alias: fieldMeta.name,
              description: fieldMeta.name,
              // set Tableau column role based off of dataType
              columnRole: (dataTypesToConsiderMetric.includes(dataType) ? tableau.columnRoleEnum.measure : tableau.columnRoleEnum.dimension),
              dataType
            }
          } else { // We'll filter these out later
            return false
          }
        })

        // Add airtable record ID
        fieldsForTableau.push({
          id: replaceSpecialCharsForTableauID(FIELD_NAME_FOR_AIRTABLE_RECORD_ID),
          dataType: tableau.dataTypeEnum.string, // determineTableauColumnType(FIELD_NAME_FOR_AIRTABLE_RECORD_ID),
          description: `Airtable Record ID from table ${tableMeta.name}`
        })

        return {
          id: replaceSpecialCharsForTableauID(tableMeta.name),
          alias: tableMeta.name,
          description: `Airtable '${tableMeta.name}' (${tableMeta.id}) from base ${BASE_ID}.`,
          columns: fieldsForTableau.filter(Boolean)
        }
      })

      // Save updated connectionData - we need this to look up additional metadata
      connectionData.TABLE_FIELD_METADATA = TABLE_FIELD_METADATA
      tableau.connectionData = JSON.stringify(connectionData)

      // Tell Tableau we're done and provide the array of schemas
      schemaCallback(tableSchemas)
    } catch (err) {
      console.error(err)
      tableau.abortWithError(`Error during getSchema: ${err.message}`)
    }
  }

  // Function called when Tableau is ready to pull the data
  tableauConnector.getData = async function (table, doneCallback) {
    try {
      console.debug('Getting data for', { table })

      // Read configuration variables and initialize Airtable client
      const { BASE_ID, FIELD_NAME_FOR_AIRTABLE_RECORD_ID, TABLE_FIELD_METADATA } = JSON.parse(tableau.connectionData)
      const airtableFieldMetaForTable = TABLE_FIELD_METADATA[table.tableInfo.alias]
      const base = new Airtable({ apiKey: tableau.password }).base(BASE_ID)

      // Create an empty array of rows we will populate and eventually provide to Tableau
      const rows = []

      // Get all records from Airtable using the REST API
      const allRecords = await base(table.tableInfo.alias).select({}).all()
      // console.debug({ allRecords })

      // Loop through each record received and construct the key-value pair in an object
      for (const record of allRecords) {
        const rowForTableau = {}

        // Go through every column present in the Tableau schema and look up the value from Airtable based off of the Tableau column's "description" which is the Airtable field name
        for (const col of table.tableInfo.columns) {
          let value
          // Check the column ID and do something special for the Airtable Record ID column
          if (col.id === replaceSpecialCharsForTableauID(FIELD_NAME_FOR_AIRTABLE_RECORD_ID)) {
            value = record.getId()
          } else {
            // Otherwise, try to get the value as a string
            try {
              // using description though `alias` would be better but for some reason Tableau doesnt always return it to us (TODO)
              const airtableFieldMeta = airtableFieldMetaForTable[col.description]
              const airtableRawValue = record.get(col.description)
              value = airtableFieldValueToTableauColumnValue(airtableRawValue, airtableFieldMeta)
            } catch (e) {
              console.error(e)
            }
          }
          rowForTableau[col.id] = value
        }

        // Add this record (tableau row) to the array of rows
        rows.push(rowForTableau)
      }

      // Append all the rows to the Tableau table
      table.appendRows(rows)

      // For debugging purposes, log the table metadata and rows we just added to it
      console.debug('Finished getData for', { table, rows })

      // Let Tableau know we're done getting data for the table requested
      doneCallback()
    } catch (err) {
      console.error(err)
      tableau.abortWithError(`Error during getData: ${err.message}`)
    }
  }

  // Register the constructed connector (with its handlers) with Tableau
  tableau.registerConnector(tableauConnector)

  // Create event listeners for when the user submits the HTML form
  $(document).ready(function () {
    const airtableApiTokenField = $('#airtableApiToken')
    const airtableSwitchBaseInput = $('#airtableSwitchBaseInput')
    const airtableBaseIdFieldId = '#airtableBaseId'

    // After waiting half a second, attempt to parse the Tableau version to determine if the user is opening from within a supported version of Tableau
    // If not, display some instructions
    setTimeout(function () {
      try {
        const version = +tableau.platformVersion.split('.').slice(0, 2).join('.')
        if (version < 2019.4) throw new Error('Tableau version must be > 2019.4')
      } catch (err) {
        console.error(err)
        $('div.formFieldAndSubmitContainer').hide()
        $('.formHeader').append("<hr /><br /><p class='warning formDescription'>Use this Web Data Connector from Tableau version 2019.4 or higher. <a href='https://tableau.github.io/webdataconnector/docs/wdc_use_in_tableau.html'>More info.</a></p>")
      }
    }, 500)

    airtableSwitchBaseInput.on('click', function (e) {
      $('#airtableBaseIdContainer').html('<input type="text" pattern="app[A-Za-z0-9]{5,}" data-parsley-errors-container="#errorsFor_airtableBaseId" data-parsley-pattern-message="Your base ID should start with the letters \'app\'" class="col-12 line-height-4 rounded border-thick border-darken2 border-darken3-hover detailCursor-border-blue border-blue-focus detailCursor-stroked-blue-focus"  value="" id="airtableBaseId" required="" style="padding: 6px" />')
      airtableSwitchBaseInput.hide()
      $('#airtableBaseIdPointer').show()
    })

    // On API token validation...
    airtableApiTokenField.parsley().on('field:success', async function (e) {
      // Add the  one base to the existing <select> drop down
      const o = new Option('Survey Results for Report - Véro', 'apptQuOD7EJrQVvfm')
      $(o).html('Survey Results for Report - Véro')
      $(airtableBaseIdFieldId).append(o)
    })

    // Form validation powered by parsleyjs.org
    $('#airtableWdcForm').parsley({
      errors: {
        container: function (elem) {
          return $(elem).parent().parent().parent()
        }
      }
    })
      .on('field:validated', function () {
        const ok = $('.parsley-error').length === 0
        $('.bs-callout-info').toggleClass('hidden', !ok)
        $('.bs-callout-warning').toggleClass('hidden', ok)
      })
      .on('form:submit', function () {
        // Store form values in Tableau connection data
        const connectionData = {
          BASE_ID: $(airtableBaseIdFieldId).val(),
          FIELD_NAME_FOR_AIRTABLE_RECORD_ID: '_airtableRecordId'
        }
        tableau.connectionData = JSON.stringify(connectionData)
        tableau.password = airtableApiTokenField.val().trim()
        tableau.connectionName = `Airtable Base ${connectionData.BASE_ID}`

        // Send the connector object to Tableau
        tableau.submit()
      })
  })
})()
