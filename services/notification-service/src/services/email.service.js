const { createLogger } = require('../../../../shared/src/utils/logger');

const logger = createLogger('EmailService');

class EmailService {
  constructor() {
    this.sentEmails = [];
  }

  async sendMatchEmail(userEmail, partnerName, matchId) {
    const subject = `🔥 It's a Match! You and ${partnerName} liked each other!`;
    const body = `
      Hi there,

      Great news! You have a new match on Tinder with ${partnerName}!
      
      Match Reference ID: ${matchId}
      
      Open Tinder now to start the conversation before the spark fades! 🔥
      
      Best,
      The Tinder Scalable Team
    `;

    const email = {
      to: userEmail,
      subject,
      body,
      sentAt: new Date(),
    };

    this.sentEmails.push(email);
    logger.info(`📧 [EMAIL SENT] To: ${userEmail} | Subject: "${subject}"`);
    return true;
  }

  async sendSwipeRightAlertEmail(userEmail) {
    const subject = `👀 Someone just swiped right on you!`;
    const body = `
      Hi there,

      Someone in your area just swiped right on your profile!
      
      Log into Tinder now and swipe right back to find out who it is and make a match!
      
      Best,
      The Tinder Scalable Team
    `;

    const email = {
      to: userEmail,
      subject,
      body,
      sentAt: new Date(),
    };

    this.sentEmails.push(email);
    logger.info(`📧 [EMAIL SENT] To: ${userEmail} | Subject: "${subject}"`);
    return true;
  }

  getSentEmails() {
    return this.sentEmails;
  }
}

const emailService = new EmailService();

module.exports = {
  emailService,
  EmailService,
};

