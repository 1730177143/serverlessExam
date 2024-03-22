import {APIGatewayProxyHandlerV2} from "aws-lambda";
import {DynamoDBClient} from "@aws-sdk/client-dynamodb";
import {DynamoDBDocumentClient, QueryCommand} from "@aws-sdk/lib-dynamodb";

const ddbDocClient = createDDbDocClient();

export const handler: APIGatewayProxyHandlerV2 = async (event) => {
    try {
        console.log("Event: ", event);
        const movieId = event.pathParameters?.movieId;
        const awardBody = event.pathParameters?.awardBody;
        const min = event.queryStringParameters?.min;
        if (!movieId) {
            return {
                statusCode: 400,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({message: "Missing or invalid movieId"}),
            };
        }

        let queryInput = {
            TableName: process.env.TABLE_NAME,
            KeyConditionExpression: "MovieId = :movieId",
            ExpressionAttributeValues: {
                ":movieId": parseInt(movieId),
            },
        };
        if (min) {
            queryInput.FilterExpression = "numAwards > :min";
            queryInput.ExpressionAttributeValues[":min"] = parseInt(min);
        }
        if (awardBody) {
            queryInput.KeyConditionExpression = "awardBody = :awardBody and movieId = :movieId";
            queryInput.ExpressionAttributeValues[":awardBody"] = awardBody;
        }

        const commandOutput = await ddbDocClient.send(new QueryCommand(queryInput));


        if (!commandOutput.Items || commandOutput.Items.length === 0) {
            return {
                statusCode: 404,
                headers: {
                    "content-type": "application/json",
                },
                body: JSON.stringify({message: "Request failed"}),
            };
        }

        return {
            statusCode: 200,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({reviews: commandOutput.Items}),
        };
    } catch (error: any) {
        console.error("Error: ", JSON.stringify(error));
        return {
            statusCode: 500,
            headers: {
                "content-type": "application/json",
            },
            body: JSON.stringify({error: error.message}),
        };
    }
};

function createDDbDocClient() {
    const ddbClient = new DynamoDBClient({region: process.env.REGION});
    const marshallOptions = {
        convertEmptyValues: true,
        removeUndefinedValues: true,
        convertClassInstanceToMap: true,
    };
    const unmarshallOptions = {
        wrapNumbers: false,
    };
    const translateConfig = {marshallOptions, unmarshallOptions};
    return DynamoDBDocumentClient.from(ddbClient, translateConfig);
}
