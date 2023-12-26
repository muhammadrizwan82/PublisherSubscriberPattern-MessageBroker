using MessageBroker.Data; 
using MessageBroker.Models;
using Microsoft.EntityFrameworkCore;

//https://www.youtube.com/watch?v=es8A7aw6Y5E&list=PLfehfHz8RuH2Y_CioLFG-ETCP3Yx9tg94&index=30
var builder = WebApplication.CreateBuilder(args);

builder.Services.AddDbContext<AppDbContext>(options => options.UseSqlite("Data Source=MessageBus.db"));

var app = builder.Build();

app.UseHttpsRedirection();

//Create Topic
app.MapPost("api/topics", async (AppDbContext context, Topic topic) =>
{
    await context.AddAsync(topic);
    await context.SaveChangesAsync();
    return Results.Created($"api/topics/{topic.Id}", topic);
});

//Return All Topics
app.MapGet("api/topics", async (AppDbContext context) =>
{
    var topics = await context.Topics.ToListAsync();
    return Results.Ok(topics);

});

app.MapPost("api/topics/{topicId}/messages", async (AppDbContext context, int topicId, Message message) =>
{
    bool topics = await context.Topics.AnyAsync(t => t.Id == topicId);
    if (!topics)
    {
        return Results.NotFound("Topic not found");
    }

    var subscriptions = context.Subscriptions.Where(t => t.TopicId == topicId);
    if (subscriptions.Count() == 0)
        return Results.NotFound("There are no subscription for this topics");

    foreach (var subscription in subscriptions)
    {
        Message msg = new Message
        {
            TopicMessage = message.TopicMessage,
            SubscriptionId = subscription.Id,
            ExpireAfter = message.ExpireAfter,
            MessageStatus = message.MessageStatus
        };
        await context.Messages.AddAsync(msg);
    }

    await context.SaveChangesAsync();

    return Results.Ok("Messages has been published");
    

});


app.MapPost("api/topics/{topicId}/subscriptions", async (AppDbContext context, int topicId, Subscription subscription) =>
{
    bool topics = await context.Topics.AnyAsync(t => t.Id == topicId);
    if (!topics)
    {
        return Results.NotFound("Topic not found");
    }
    subscription.TopicId = topicId;
    await context.Subscriptions.AddAsync(subscription);
    await context.SaveChangesAsync();

    return Results.Created($"api/topics/{topicId}/subscriptions/{subscription.Id}", subscription);


});

//Get Subscriber Message
app.MapGet("api/subscriptions/{subscriptionId}", async (AppDbContext context, int subscriptionId) =>
{
    bool subscription = await context.Subscriptions.AnyAsync(t => t.Id == subscriptionId);
    if (!subscription)
    {
        return Results.NotFound("Subscription not found");
    }    
    var messages = context.Messages.Where(msg=> msg.SubscriptionId == subscriptionId && msg.MessageStatus != "SENT");
    if(messages.Count() == 0)
        return Results.NotFound("No new messages");

    foreach (var message in messages) {
        message.MessageStatus = "REQUESTED";        
    }
    await context.SaveChangesAsync();
    return Results.Ok(messages);

});

//Get Subscriber Message
app.MapPost("api/subscriptions/{subscriptionId}/messages", async (AppDbContext context, int subscriptionId,int[] messageIds) =>
{
    bool subscription = await context.Subscriptions.AnyAsync(t => t.Id == subscriptionId);
    if (!subscription)
    {
        return Results.NotFound("Subscription not found");
    }

    if (messageIds.Length <= 0) {
        return Results.BadRequest();
    }

    int count = 0;
    foreach (var messageId in messageIds) {
        var message = context.Messages.FirstOrDefault(x => x.Id == messageId);
        if (message != null) {
            message.MessageStatus = "SENT";
            await context.SaveChangesAsync();
            count++;
        }
    }

    return Results.Ok($"Acknowledged {count}/{messageIds.Length} messages");
});

app.Run();
