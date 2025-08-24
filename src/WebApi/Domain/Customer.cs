namespace WebApi.Domain;

public sealed record Customer(string FirstName, string LastName, string Email, DateTime BirthDate);
