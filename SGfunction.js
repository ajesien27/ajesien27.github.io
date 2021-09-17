'use strict';
// Handle unsupported event types
async function onGroup(event, settings) {
	throw new EventNotSupported('Group event is not supported');
}
async function onPage(event, settings) {
	throw new EventNotSupported('Page event is not supported');
}
async function onScreen(event, settings) {
	throw new EventNotSupported('Screen event is not supported');
}
async function onAlias(event, settings) {
	throw new EventNotSupported('Alias event is not supported');
}
async function onDelete(event, settings) {
	throw new EventNotSupported('Delete request is not supported');
}
async function onTrack(event, settings) {
	throw new EventNotSupported('Track request is not supported');
}
/*
Settings:
- syncedTraits (array): array of user profile traits to be synced along with the audience
- personasProfileApiKey (string): Personas Profile API key
- sendgridApiKey (string): SendGrid API key
- personasSpaceId (string): Personas space Id
*/
async function onIdentify(event, settings) {
	return onBatch([event], settings);
}

async function onBatch(events, settings) {
	console.time('func');

	let batch = events;

	// List of Sendgrid reserved fields
	const reservedFields = [
		'first_name',
		'last_name',
		'email',
		'alternate_emails',
		'address_line_1',
		'address_line_2',
		'city',
		'state_province_region',
		'postal_code',
		'country',
		'alternate_emails',
		'phone_number',
		'whatsapp',
		'line',
		'facebook',
		'unique_name',
		'lists',
		'created_at',
		'updated_at',
		'last_emailed',
		'last_clicked',
		'last_opened'
	];

	// List of traits to be synced along with the audience
	var syncedTraitsStr = settings.syncedTraits.map(function(item) {
		return item
			.replace(' ', '_')
			.toLowerCase()
			.trim();
	});

	// Pull the list of all available custom fields in Sendgrid
	let sendgridFields = await getSendgridFields(settings);

	// Pull traits from Personas for each individual identify() event
	let personasTraitsObjects = batch.map(event =>
		getPersonasTraits(event, settings)
	);

	let userTraits = await Promise.all(personasTraitsObjects);

	// Map user's traits to Sendgrid custom fields and create a Sendgrid request body
	let sendgridContactsArray = userTraits.map(validUserProfile =>
		mapSendgridRequestBody(
			validUserProfile,
			sendgridFields,
			reservedFields,
			syncedTraitsStr
		)
	);

	// Send a request to Sendgrid
	let updateSendgrid = await updateSendgridContacts(
		sendgridContactsArray,
		settings
	);

	console.time('func');
}

// Pull the list of custom fields from Sendgrid ONCE
// Returns the object of all available Sendgrid custom fields: {custom_field_name : custom_field_id}
async function getSendgridFields(settings) {
	let sendgridFieldUrl = `https://api.sendgrid.com/v3/marketing/field_definitions`;

	const sendgridFieldReq = await fetch(sendgridFieldUrl, {
		headers: new Headers({
			Authorization: 'Bearer ' + settings.sendgridApiKey,
			'Content-Type': 'application/json'
		}),

		method: 'get'
	});

	if (sendgridFieldReq.ok) {
		var sendgridFields = await sendgridFieldReq.json();
	} else {
		console.log(sendgridFieldReq);
		console.log(sendgridFieldReq.status + ' ' + sendgridFieldReq.message);
		if (sendgridFieldReq.status >= 500 || sendgridFieldReq.status === 429) {
			throw new RetryError(
				`Sendgrid get custom fields retryable error: ${sendgridFieldReq.status}`
			);
		} else {
			throw new Error(
				'Sendgrid get custom fields non-retryable error ' +
					sendgridFieldReq.status +
					' ' +
					sendgridFieldReq.statusText
			);
		}
	}
	// If Sendgrid doesn't have any custom fields yet - return array [ {undefined: undefined} ]
	let sendgridCustomFields =
		sendgridFields.custom_fields === undefined
			? [
					{
						undefined: undefined
					}
			  ]
			: sendgridFields.custom_fields;
	obj = {};
	sendgridCustomFields.forEach(field => {
		obj[field.name] = field.id;
	});

	return obj; // Format: {custom_field_name : custom_field_id}
}

// Handler function returns the list of Personas traits for a user. Triggered for each individual event
// Returns traits object {trait_name: value}
async function getPersonasTraits(event, settings) {
	let profileApiBaseUrl = `https://profiles.segment.com/v1/spaces/`;
	let profileApiMidUrl = `/collections/users/profiles/`;

	// Pull user traits either by userId or email address whichever is available in the identify()
	let identifier = event.userId || event.traits.email;
	const prefix = event.userId !== undefined ? 'user_id:' : 'email:';

	let profileApiUrl =
		profileApiBaseUrl +
		settings.personasSpaceId +
		profileApiMidUrl +
		prefix +
		identifier +
		'/traits?limit=200';

	const personasReq = await fetch(profileApiUrl, {
		headers: new Headers({
			Authorization: 'Basic ' + btoa(settings.personasProfileApiKey + ':'),
			'Content-Type': 'application/json'
		}),
		method: 'get'
	});
	if (personasReq.ok) {
		let personasUser = await personasReq.json();
		var allPersonasTraits = personasUser.traits;
		// Append audience name to the list of user's traits
		if (
			event.context.personas !== undefined &&
			event.context.personas.computation_class == 'audience' &&
			event.context.personas.computation_key !== undefined
		) {
			let audienceName = event.context.personas.computation_key;
			let audienceValue = allPersonasTraits[audienceName];
			allPersonasTraits['audience'] = {};
			allPersonasTraits.audience[audienceName] = audienceValue;
		}
		return allPersonasTraits;
	} else {
		if (personasReq.status >= 500 || personasReq.status == 429) {
			console.log(
				'Personas Profile API (/traits) error. Function will be retried. Status: ' +
					personasReq.status +
					' , ' +
					personasReq.statusText +
					'.\n User ' +
					prefix +
					identifier
			);
			throw new RetryError(
				'Personas Profile API (/traits) error. Function will be retried. Status: ' +
					personasReq.status +
					' , ' +
					personasReq.statusText +
					'.\n User ' +
					prefix +
					identifier
			);
		} else {
			console.log(
				'Personas Profile API (/traits) error, exiting. Status: ' +
					personasReq.status +
					' , ' +
					personasReq.statusText +
					'.\n User ' +
					prefix +
					identifier
			);
			throw new Error(
				'Personas Profile API (/traits) error, exiting. Status: ' +
					personasReq.status +
					' , ' +
					personasReq.statusText +
					'.\n User ' +
					prefix +
					identifier
			);
		}
	}
}

function mapSendgridRequestBody(
	allPersonasTraits,
	sendgridFields,
	reservedFields,
	syncedTraitsStr
) {
	let audience = Object.keys(allPersonasTraits.audience)[0];

	console.log(JSON.stringify(allPersonasTraits));

	// Drop any Personas traits which are not either:
	//   - mapped to Reserved Fields
	//   - not included in the Synced Traits setting
	//   - not matching audience name
	// Produce object {trait_name : value}
	Object.keys(allPersonasTraits).forEach(traitName => {
		if (
			!syncedTraitsStr.includes(traitName) &&
			!reservedFields.includes(traitName) &&
			traitName !== audience
		) {
			delete allPersonasTraits[traitName];
		}
	});

	console.log(JSON.stringify(allPersonasTraits));

	// Cleaned up traits: all traits matching Reserved fields, audience name, synced traits
	let filteredTraits = allPersonasTraits;

	// Map traits to SendGrid custom fields via name (except reserved fields & audience name)
	let personasTraitsToFields = {};

	// Match all traits synced_traits and audience to sendgrid fields
	// Produce {trait_name: field_id}
	Object.keys(filteredTraits).forEach(traitName => {
		if (
			(syncedTraitsStr.includes(traitName) || traitName == audience) &&
			!reservedFields.includes(traitName)
		) {
			personasTraitsToFields[traitName] = sendgridFields[traitName];
		}
	});
	// console.log(personasTraitsToFields);
	var missingFields = {};

	// 1. Pull all keys with 'undefined' values from 'traitsToFields' map: all traits (key) which do not have a corresponding custom field (value)
	// 2. Exclude reserved field names
	// 3. Include only tratis specified by the user (settings.syncedTraits)
	Object.keys(personasTraitsToFields).forEach(traitName => {
		// If a trait is listed in SyncedTraits setting, doesn't have a matching custom field and cannot be mapped to a reserved field =>
		// then include it to the MissingFields object: {trait name: trait value}.
		if (
			personasTraitsToFields[traitName] === undefined
			//&&!reservedFields.includes(traitName.toString()) &&
			//syncedTraitsStr.includes(traitName)
		) {
			missingFields[traitName] = filteredTraits[traitName];
		}
	});

	if (Object.keys(missingFields).length > 0) {
		throw new Error('Custom fields not found in Sendgrid: ' + missingFields);
	}

	// Produce {field_id : trait_value}
	let fieldsToTraits = {};
	Object.keys(personasTraitsToFields).forEach(key => {
		fieldsToTraits[personasTraitsToFields[key]] = valToStr(filteredTraits[key]);
	});

	let sendgridReqBodyCustomFields = fieldsToTraits;

	let sendgridRequestBodyReservedFields = {};
	// Map a few reserved fields

	if (filteredTraits.email !== undefined) {
		sendgridRequestBodyReservedFields[
			'email'
		] = filteredTraits.email.toLowerCase();
	}

	let sendgridReqBody = {
		...sendgridRequestBodyReservedFields,
		custom_fields: { ...sendgridReqBodyCustomFields }
	};

	return sendgridReqBody;
}

async function updateSendgridContacts(contactsArray, settings) {
	let sendgridUrl = `https://api.sendgrid.com/v3/marketing/contacts`;

	var contactCount = contactsArray.length;

	let requestBody = {
		contacts: contactsArray
	};

	console.log(JSON.stringify(requestBody));
	const sendgridResponse = await fetch(sendgridUrl, {
		headers: new Headers({
			Authorization: 'Bearer ' + settings.sendgridApiKey,
			'Content-Type': 'application/json'
		}),
		body: JSON.stringify(requestBody),
		method: 'put'
	});

	// Sendgrid API returns 202 (accepted) or 40x (error). Both responses have a JSON body.
	// Success response includes job_id. Error response includes error object and an array of errors

	if (sendgridResponse.ok) {
		// Success
		let sendgridResponseJson = await sendgridResponse.json();
		console.log(
			'Sendgrid contact update request OK. ' +
				sendgridResponse.status +
				' ' +
				sendgridResponse.statusText +
				'\nSendgrid contact update request body: ' +
				JSON.stringify(requestBody) +
				'\nSendgrid job_id: ' +
				sendgridResponseJson.job_id +
				'\nContacts sent: ' +
				contactCount
		);
	} else {
		// Retryable errors
		if (sendgridResponse.status >= 500 || sendgridResponse.status === 429) {
			console.log(
				'Sendgrid contact update error: rate limit. Function will be retried. ' +
					sendgridResponse.status +
					' ' +
					sendgridResponse.statusText +
					'\nSendgrid contact update request body: ' +
					JSON.stringify(requestBody) +
					'\nContacts sent: ' +
					contactCount
			);
			throw new RetryError(
				'Sendgrid contact update error: rate limit. Function will be retried. ' +
					sendgridResponse.status +
					' ' +
					sendgridResponse.statusText +
					'\nSendgrid contact update request body: ' +
					JSON.stringify(requestBody) +
					'\nContacts sent: ' +
					contactCount
			);
		}
		// Bad request (returns job_id according to docs). Non-retryable
		if (sendgridResponse.status === 400) {
			let sendgridErrorResponse = await sendgridResponse.json();
			console.log(
				'Sendgrid contacts endpoint error. Function will not be retried.' +
					sendgridResponse.status +
					' ' +
					sendgridResponse.statusText +
					'\nSendgrid contact update request body: ' +
					JSON.stringify(requestBody) +
					'\nSendgrid job_id: ' +
					sendgridErrorResponse.job_id +
					'\nContacts sent: ' +
					contactCount
			);
			throw new ValidationError(
				'Sendgrid contacts endpoint error. Function will not be retried.' +
					sendgridResponse.status +
					' ' +
					sendgridResponse.statusText +
					'\nSendgrid contact update request body: ' +
					JSON.stringify(requestBody) +
					'\nSendgrid job_id: ' +
					sendgridErrorResponse.job_id +
					'\nContacts sent: ' +
					contactCount
			);
		} else {
			// All other errors
			console.log(
				'Sendgrid error: ' +
					sendgridResponse.status +
					' ' +
					sendgridResponse.statusText +
					'\nSendgrid contact update request body: ' +
					JSON.stringify(requestBody) +
					'\nContacts sent: ' +
					contactCount
			);
			throw new Error(
				'Sendgrid contacts endpoint error. Function will not be retried.' +
					sendgridResponse.status +
					' ' +
					sendgridResponse.statusText +
					'\nSendgrid contact update request body: ' +
					JSON.stringify(requestBody) +
					'\nContacts sent: ' +
					contactCount
			);
		}
	}
}

// Helper function to return 3 data types of Sendgrid custom fields: Text, Date or Number
// Docs: https://sendgrid.com/docs/API_Reference/api_v3.html
function getSendgridTypeName(value) {
	const dateRe = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.\d*Z/;
	if (typeof value === 'number') {
		return 'Number';
	} else if (typeof value === 'string' && value.search(dateRe) == 0) {
		return 'Date';
	} else {
		return 'Text';
	}
}
// Helper function which stringifies true/false boolean values and joins arrays for Sendgrid
// Sendgrid doesn't have 'boolean' type for custom fields, bools and arrays are stored as string
function valToStr(value) {
	if (typeof value === 'boolean') return String(value);
	if (typeof value === 'string') return value;
	if (typeof value === 'number') return value;
	if (value == null) return '';
	if (typeof value === 'object' && value !== null) return value.join();
}