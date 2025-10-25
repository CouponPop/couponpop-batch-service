# Gemini Instruction Guide

## Naming Conventions

* **from**: Convert one parameter into an object.

  ```java
  UserResponseDto.from(user);
  Instant instant = Instant.from(dateTime);
  ```

* **of**: Combine or aggregate multiple parameters.

  ```java
  EnumSet.of(Role.ADMIN, Role.USER);
  LocalDate.of(2025, 9, 3);
  ```

* **valueOf**: Similar to `from`/`of`, but provides a more descriptive name.

  ```java
  BigInteger.valueOf(12345);
  String.valueOf(true);
  ```

* **getInstance**: Return an instance, but not always new. Often used in Singleton pattern.

  ```java
  Calendar.getInstance();
  NumberFormat.getInstance();
  ```

* **create / newInstance**: Always return a new object.

  ```java
  Array.newInstance(String.class, 10);
  ```

* **get[Type]**: Returns an object of another class type.

  ```java
  Files.getFileStore(path);
  ```

* **new[Type]**: Returns an object of another class type, always new.

  ```java
  Files.newBufferedReader(path);
  ```

---

## Commenting Rules

* Use one-line comments when simple.
* Use multi-line comments (`/** ... */`) for class-level or methods that are referenced externally (Javadoc style).

```java
/**
 * Used when methods or classes must be referenced externally.
 * Accessible via Javadoc.
 */

// Otherwise, use one-line comments.
```

---

## Method Rules

* **0–2 parameters**: Write in one line if possible.

  ```java
  public ResponseEntity<UserResponse> getUserInfo(@PathVariable Long userId) {
      // code
  }
  ```

* **3+ parameters**: Align based on the first parameter.

  ```java
  public ResponseEntity<PostListResponse> getPostsByPeriod(@RequestParam LocalDateTime startDate,
                                                           @RequestParam LocalDateTime endDate,
                                                           @RequestParam(defaultValue = "0") int page) {
      // code
  }
  ```

* **DTO / Entity formatting**:

  ```java
  public record UserResponse(
      @Size(max = 20)
      Long id,

      @Annotation
      String name
  ) {}
  ```

* **Intermediate variables**: Use descriptive names for clarity.

  ```java
  UserResponse updatedUser = userService.updateUserInfo(user.getId(), request);
  return ResponseEntity.ok(updatedUser);
  ```

* **Line breaks**:

  * Between method header and body.
  * Between logical steps inside methods.
  * Before `return` or `save` statements.

  ```java
  if (userRepository.findByEmail(signupRequest.email()).isPresent())
      throw new IllegalArgumentException("이미 존재하는 이메일입니다.");

  User user = userMapper.toEntity(signupRequest);
  user.updatePassword(passwordEncoder.encode(signupRequest.password()));

  userRepository.save(user);

  return userMapper.toResponse(user);
  ```

* **Curly braces**: Always use, even for one-line methods or `if` statements.

  ```java
  if (a == b) {
      return true;
  }
  ```

---

## DTO Rules

* **Request DTO → Entity**: Use private constructor + builder, expose only static factory methods.

  ```java
  @Builder(access = AccessLevel.PRIVATE)
  private User(String username, String name, String email, String password, UserRole role) {
      this.username = username;
      this.name = name;
      this.email = email;
      this.password = password;
      this.role = role;
  }

  public static User signUp(String username, String name, String email, String password, UserRole role) {
      return User.builder()
          .username(username)
          .name(name)
          .email(email)
          .password(password)
          .role(role)
          .build();
  }
  ```

* **Entity → Response DTO**: Use `from` or `of`.

  ```java
  public static AuthRegisterResponse from(User user) {
      return new AuthRegisterResponse(
          user.getId(),
          user.getUsername(),
          user.getEmail(),
          user.getName(),
          user.getRole(),
          user.getCreatedAt()
      );
  }
  ```
